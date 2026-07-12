/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce.core

import akka.actor.typed.scaladsl.AskPattern.*
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior, Scheduler}
import akka.util.Timeout
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

import scala.concurrent.*
import scala.util.*

/**
 * The command protocol handled by a Master actor.
 *
 * A Master accepts its input in either of two shapes -- as a Map[K1,V1] or as a Seq[(K1,V1)] --
 * mirroring the two message shapes the original (classic-actor) Master.receive matched on.
 * The "First" variants (Master_First / Master_First_Fold) reuse this same protocol with K1=Unit,
 * exactly as the original MasterBaseFirst reused MasterBase[Unit,...] internally: their public
 * factory functions unitize the incoming Seq[V1] into a Seq[(Unit,V1)] before it is sent here.
 *
 * NOTE: unlike the classic actor (which replied with `Status.Failure` on a hard failure), a
 * Typed reply can only ever be the declared type. So the reply type here is `Try[Response[K2,V2]]`
 * rather than a bare `Response[K2,V2]` -- this preserves the original behavior of failing the
 * caller's ask with the *original* exception, rather than letting it time out.
 *
 * @tparam K1 key type: the message which this actor responds to is of type Map[K1,V1].
 * @tparam V1 input type: the message which this actor responds to is of type Seq[V1].
 * @tparam K2 key type: mapper groups things by this key and reducer processes said groups.
 * @tparam V2 output type: the message which is sent on completion to the sender is of type Response[K2,V2]
 */
sealed trait MasterCommand[K1, V1, K2, V2]

/** Command carrying a Map[K1,V1] payload, replying to `replyTo` with a Try[Response[K2,V2]]. */
final case class ComputeMap[K1, V1, K2, V2](m: Map[K1, V1], replyTo: ActorRef[Try[Response[K2, V2]]]) extends MasterCommand[K1, V1, K2, V2]

/** Command carrying a Seq[(K1,V1)] payload, replying to `replyTo` with a Try[Response[K2,V2]]. */
final case class ComputeSeq[K1, V1, K2, V2](s: Seq[(K1, V1)], replyTo: ActorRef[Try[Response[K2, V2]]]) extends MasterCommand[K1, V1, K2, V2]

/** Command to stop this Master -- its mapper and reducer children are stopped transitively. */
final case class CloseMaster[K1, V1, K2, V2]() extends MasterCommand[K1, V1, K2, V2]

/**
 * @author scalaprof
 * @param config an instance of Config which defines a suitable configuration
 * @param f      the mapper function which takes a (K1,V1) pair and creates a key-value tuple of type (K2,W), wrapped in Try
 * @param g      the reducer function which combines two values (a V2 and a W) into one V2
 */
object Master:
  private val logger = LoggerFactory.getLogger("com.phasmid.majabigwaduce.core.Master")

  def apply[K1, V1, K2, W, V2 >: W](config: Config, f: (K1, V1) => Try[(K2, W)], g: (V2, W) => V2): Behavior[MasterCommand[K1, V1, K2, V2]] =
    behavior(config, f, Reducer(g))

  /**
   * Builds the shared Master behavior. On setup, spawns `mappers` (per config) mapper children
   * and `reducers` (per config) reducer children, then handles ComputeMap/ComputeSeq/Close
   * commands by driving the map -> distribute -> reduce -> collate pipeline and replying to
   * each request's `replyTo`.
   *
   * Shared by Master, Master_Fold, Master_First and Master_First_Fold -- the only difference
   * between them is which reducer Behavior is spawned (plain Reducer vs Reducer_Fold), and, for
   * the "First" variants, that K1=Unit and `f` has already been unitized by the caller.
   */
  private[core] def behavior[K1, V1, K2, W, V2](config: Config, f: (K1, V1) => Try[(K2, W)], reducerBehavior: Behavior[ReducerCommand[K2, W, V2]]): Behavior[MasterCommand[K1, V1, K2, V2]] =
    MapReduceActor.withLifecycle(logger) { context =>
      val actors = Actors(context.system, config)

      given timeout: Timeout = Actors.getTimeout(config.getString("timeout"))
      logger.debug(s"Master: timeout=$timeout")

      given ec: ExecutionContext = context.executionContext
      given scheduler: Scheduler = context.system.scheduler

      // NOTE: the mappers and reducers will be terminated when this master is terminated.
      val mapperBehavior = if Master.isForgiving(config) then Mapper_Forgiving(f) else Mapper(f)
      val nMappers = config.getInt("mappers")
      logger.debug(s"creating $nMappers mappers")
      val mappers: Seq[ActorRef[MapperCommand[K1, V1, K2, W]]] =
        for i <- 1 to nMappers yield
          actors.createActor[MapperCommand[K1, V1, K2, W]]((b, n) => context.spawn(b, n), Some(s"${Master.sMpr}-$i"), mapperBehavior)

      val nReducers = config.getInt("reducers")
      logger.debug(s"creating $nReducers reducers")
      val reducers: Seq[ActorRef[ReducerCommand[K2, W, V2]]] =
        for i <- 1 to nReducers yield
          actors.createActor[ReducerCommand[K2, W, V2]]((b, n) => context.spawn(b, n), Some(s"${Master.sReducer}-$i"), reducerBehavior)
      if Master.isForgiving(config) then logger.debug("setting forgiving mode")

      // Splits the incoming batch across the mapper pool and fans out one DoMap per chunk,
      // mirroring distributeWork/doReductionAsync below -- but by position, not by key, since
      // there's no key yet at this stage (that's what mapping produces). Unlike the reduce
      // side, chunks.length <= mappers.length always holds by construction (ceil-division), so
      // a plain zip suffices -- no round-robin cycling is needed here.
      // NOTE this involves a cast to the parametric type Z which can result in a ClassCastException,
      // the same way the classic-actor `ask.mapTo[Z]` did.
      def doMap(i: KeyValuePairs[K1, V1]): Future[Map[K2, Seq[W]]] =
        val chunks = Master.splitIntoChunks(i.m, mappers.length)
        val responses: Seq[Future[MapperResponse[K2, W]]] =
          for (chunk, mapperRef) <- chunks.zip(mappers) yield
            mapperRef.ask[MapperResponse[K2, W]](replyTo => DoMap(KeyValuePairs(chunk), replyTo))
        Future.sequence(responses).map(Master.mergeMapperResponses).flatMap {
          case MapperResponse(m, xs) =>
            if xs.nonEmpty && !Master.isForgiving(config)
            then Future.failed[Map[K2, Seq[W]]](xs.head)
            else
              xs.foreach(x => actors.logException("mapper exception", x))
              Future.successful(m)
        }

      // NOTE that this method operates in real time, without the protection of Try
      def distributeWork(wsK2m: Map[K2, Seq[W]]): Seq[((K2, Seq[W]), ActorRef[ReducerCommand[K2, W, V2]])] =
        val rs = LazyList.continually(reducers.to(LazyList)).flatten
        val wsK2s = for (k2, ws) <- wsK2m.to(Seq) yield (k2, ws)
        wsK2s zip rs

      def doReductionAsync(k2: K2, ws: Seq[W], actor: ActorRef[ReducerCommand[K2, W, V2]]): Future[(K2, Either[Throwable, V2])] =
        actor.ask[ReduceResult[K2, V2]](replyTo => DoReduce(Intermediate(k2, ws), replyTo)).map(r => r.k2 -> r.result)

      def doDistributeReduceCollate(wsK2m: Map[K2, Seq[W]]): Future[Map[K2, Either[Throwable, V2]]] =
        if wsK2m.isEmpty then
          logger.warn("mapper returned empty map" + (if Master.isForgiving(config) then "" else ": see log for problem and consider using Mapper_Forgiving instead"))
        val v2XeK2fs = for ((k2, ws), a) <- distributeWork(wsK2m) yield doReductionAsync(k2, ws, a)
        Future.sequence(v2XeK2fs).map(_.toMap)

      /**
       * The main map-reduce method.
       * This takes a KeyValuePairs object and returns a map of K2 and either a Throwable or a V2, all wrapped in Future.
       *
       * CONSIDER why are we using Either[Throwable, V2] instead of Try[V2]?
       */
      def doMapReduce(i: KeyValuePairs[K1, V1]): Future[Map[K2, Either[Throwable, V2]]] = for
        wsK2m <- doMap(i)
        v2XeK2m <- doDistributeReduceCollate(wsK2m)
      yield v2XeK2m

      {
        case ComputeMap(m, replyTo) =>
          logger.debug(s"Master received Map[K1,V1]: with ${m.size} elements")
          doMapReduce(KeyValuePairs.map(m)).onComplete {
            case s@Success(v2XeK2m) =>
              MapReduceActor.maybeLog(logger, "response: {}", v2XeK2m)
              replyTo ! s.map(Response.create)
            case f@Failure(x) =>
              logger.error("no response--failure", x)
              replyTo ! f.map(Response.create)
          }
          Behaviors.same
        case ComputeSeq(s, replyTo) =>
          logger.debug(s"Master received Seq[(K1,V1)]: with ${s.length} elements")
          doMapReduce(KeyValuePairs[K1, V1](s)).onComplete { t => replyTo ! t.map(Response.create) }
          Behaviors.same
        case CloseMaster() =>
          actors.close()
          Behaviors.stopped
      }
    }

  /**
   * Splits `xs` into at most `n` contiguous chunks (never more than `n`, by construction of the
   * ceiling division below). Used to fan an incoming batch out across the mapper pool.
   *
   * @param xs the input sequence.
   * @param n  the desired (maximum) number of chunks.
   * @return a sequence of at most `n` non-empty chunks; empty if `xs` is empty.
   */
  private[core] def splitIntoChunks[A](xs: Seq[A], n: Int): Seq[Seq[A]] =
    if xs.isEmpty || n <= 1 then Seq(xs).filter(_.nonEmpty)
    else
      val chunkSize = math.ceil(xs.length / n.toDouble).toInt
      xs.grouped(chunkSize).toSeq

  /**
   * Merges the `MapperResponse`s from each mapper chunk back into one: deep-merges the
   * `Map[K2,Seq[W]]` results (a key can appear in more than one chunk's result, so same-key
   * sequences are concatenated, not overwritten) and concatenates the `exceptions` lists.
   *
   * @param responses one MapperResponse per mapper chunk.
   * @return the single, combined MapperResponse.
   */
  private[core] def mergeMapperResponses[K2, W](responses: Seq[MapperResponse[K2, W]]): MapperResponse[K2, W] =
    val merged: Map[K2, Seq[W]] =
      responses.foldLeft(Map.empty[K2, Seq[W]]) { (acc, r) =>
        r.result.foldLeft(acc) { case (m, (k2, ws)) =>
          m.updated(k2, m.getOrElse(k2, Nil) ++ ws)
        }
      }
    val exceptions: Seq[Throwable] = responses.flatMap(_.exceptions)
    MapperResponse(merged, exceptions)

  /**
   * Returns the zero value of the specified type `V`. The generic zero value is
   * obtained by casting the integer 0 to the provided type `V`.
   * TESTME
   *
   * @tparam V the type for which the zero value is desired
   * @return the zero value of type `V`
   */
  def zero[V](): V = 0.asInstanceOf[V]

  /**
   * method isForgiving which looks up the value of the forgiving property of the configuration.
   *
   * @param config an instance of Config which defines a suitable configuration
   * @return true/false according to the property's value in config
   */
  def isForgiving(config: Config): Boolean = config.getBoolean("forgiving")

  /**
   * Method unitize which takes a function A=>B and returns a (Unit,A)=>B
   *
   * @param f the function to be lifted
   * @tparam A input type: the input type of the function f.
   * @tparam B output type: the output type of the function f.
   * @return a function of (Unit,A)=>B
   */
  def unitize[A, B](f: A => B): (Unit, A) => B =
    (_, v) => f(v)

  //noinspection SpellCheckingInspection
  val sReducer = "rdcr"
  val sMpr = "mpr"

/**
 * @tparam K1 key type: the message which this actor responds to is of type Map[K1,V1].
 * @tparam V1 input type: the message which this actor responds to is of type Seq[V1].
 * @tparam K2 key type: mapper groups things by this key and reducer processes said groups.
 * @tparam W  transitional type -- used internally
 * @tparam V2 output type: the message which is sent on completion to the sender is of type Response[K2,V2]
 * @param config an instance of Config which defines a suitable configuration
 * @param f      the mapper function which takes a (K1,V1) pair and creates a key-value tuple of type (K2,W), wrapped in Try
 * @param g      the reducer function which combines two values (a V2 and a W) into one V2
 * @param z      the "zero" or "unit" (i.e. initializer) function which creates an "empty" V2.
 */
object Master_Fold:
  def apply[K1, V1, K2, W, V2](config: Config, f: (K1, V1) => Try[(K2, W)], g: (V2, W) => V2, z: () => V2): Behavior[MasterCommand[K1, V1, K2, V2]] =
    Master.behavior[K1, V1, K2, W, V2](config, f, Reducer_Fold(g, z()))

/**
 * @tparam V1 input type: the message which this actor responds to is of type Seq[V1].
 * @tparam K2 key type: mapper groups things by this key and reducer processes said groups.
 * @tparam W  transitional type -- used internally
 * @tparam V2 output type: the message which is sent on completion to the sender is of type Response[K2,V2]
 * @param config an instance of Config which defines a suitable configuration
 * @param f      the mapper function which takes a V1 and creates a key-value tuple of type (K2,W), wrapped in Try
 * @param g      the reducer function which combines two values (a V2 and a W) into one V2
 */
object Master_First:
  def apply[V1, K2, W, V2 >: W](config: Config, f: V1 => Try[(K2, W)], g: (V2, W) => V2): Behavior[MasterCommand[Unit, V1, K2, V2]] =
    Master.behavior[Unit, V1, K2, W, V2](config, Master.unitize(f), Reducer(g))

/**
 * @tparam V1 input type: the message which this actor responds to is of type Seq[V1].
 * @tparam K2 key type: mapper groups things by this key and reducer processes said groups.
 * @tparam W  transitional type -- used internally
 * @tparam V2 output type: the message which is sent on completion to the sender is of type Response[K2,V2]
 * @param config an instance of Config which defines a suitable configuration
 * @param f      the mapper function which takes a V1 and creates a key-value tuple of type (K2,W), wrapped in Try
 * @param g      the reducer function which combines two values (a V2 and a W) into one V2
 * @param z      the "zero" or "unit" (i.e. initializer) function which creates an "empty" V2.
 */
object Master_First_Fold:
  def apply[V1, K2, W, V2](config: Config, f: V1 => Try[(K2, W)], g: (V2, W) => V2, z: () => V2): Behavior[MasterCommand[Unit, V1, K2, V2]] =
    Master.behavior[Unit, V1, K2, W, V2](config, Master.unitize(f), Reducer_Fold(g, z()))

/**
 * Case class used to package a response from an actor.
 *
 * @param left  a map of key-value pairs where the value is a Throwable.
 * @param right a map of key-value pairs where the value is a value.
 * @tparam K the key type.
 * @tparam V the value type.
 */
case class Response[K, V](left: Map[K, Throwable], right: Map[K, V]):
  override def toString = s"left: $left; right: $right"

  /**
   * Returns the number of key-value pairs present in the `right` map.
   *
   * @return the size of the `right` map.
   */
  def size: Int = right.size

/**
 * Factory object for the Response case class.
 *
 * Provides functionality to create a Response instance by processing a map of
 * key-value pairs where the values are wrapped in an `Either`. The left side of the
 * `Either` represents an error (Throwable), while the right side represents a valid value.
 *
 * @tparam K the key type.
 * @tparam V the value type.
 */
object Response:

  import FP.*

  def create[K, V](vXeKm: Map[K, Either[Throwable, V]]): Response[K, V] =
    invokeTupled(toMap(sequenceLeftRight(vXeKm)))(apply)
