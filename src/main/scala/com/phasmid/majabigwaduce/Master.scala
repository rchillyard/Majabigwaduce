/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce

import akka.actor.{ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.Config

import scala.concurrent._
import scala.reflect.ClassTag
import scala.util._

/**
  * @author scalaprof
  * @tparam K1 key type: the message which this actor responds to is of type Map[K1,V1].
  * @tparam V1 input type: the message which this actor responds to is of type Seq[V1].
  * @tparam K2 key type: mapper groups things by this key and reducer processes said groups.
  * @tparam W  transitional type -- used internally
  * @tparam V2 output type: the message which is sent on completion to the sender is of type Response[K2,V2]
  * @param config an instance of Config which defines a suitable configuration
  * @param f      the mapper function which takes a V1 and creates a key-value tuple of type (K2,W), wrapped in Try
  * @param g      the reducer function which combines two values (an V2 and a W) into one V2
  */
class Master[K1, V1, K2, W, V2 >: W](config: Config, f: (K1, V1) => Try[(K2, W)], g: (V2, W) => V2) extends MasterBase[K1, V1, K2, W, V2](config, f, g, Master.zero) with ByReduce[K1, V1, K2, W, V2]

/**
  * @tparam K1 key type: the message which this actor responds to is of type Map[K1,V1].
  * @tparam V1 input type: the message which this actor responds to is of type Seq[V1].
  * @tparam K2 key type: mapper groups things by this key and reducer processes said groups.
  * @tparam W  transitional type -- used internally
  * @tparam V2 output type: the message which is sent on completion to the sender is of type Response[K2,V2]
  * @param config an instance of Config which defines a suitable configuration
  * @param f      the mapper function which takes a V1 and creates a key-value tuple of type (K2,W), wrapped in Try
  * @param g      the reducer function which combines two values (an V2 and a W) into one V2
  * @param z      the "zero" or "unit" (i.e. initializer) function which creates an "empty" V2.
  */
class Master_Fold[K1, V1, K2, W, V2](config: Config, f: (K1, V1) => Try[(K2, W)], g: (V2, W) => V2, z: () => V2) extends MasterBase[K1, V1, K2, W, V2](config, f, g, z) with ByFold[K1, V1, K2, W, V2]

/**
  * @tparam V1 input type: the message which this actor responds to is of type Seq[V1].
  * @tparam K2 key type: mapper groups things by this key and reducer processes said groups.
  * @tparam W  transitional type -- used internally
  * @tparam V2 output type: the message which is sent on completion to the sender is of type Response[K2,V2]
  * @param config an instance of Config which defines a suitable configuration
  * @param f      the mapper function which takes a V1 and creates a key-value tuple of type (K2,W), wrapped in Try
  * @param g      the reducer function which combines two values (an V2 and a W) into one V2
  */
class Master_First[V1, K2, W, V2 >: W](config: Config, f: V1 => Try[(K2, W)], g: (V2, W) => V2) extends MasterBaseFirst[V1, K2, W, V2](config, f, g, Master.zero) with ByReduce[Unit, V1, K2, W, V2]

/**
  * @tparam V1 input type: the message which this actor responds to is of type Seq[V1].
  * @tparam K2 key type: mapper groups things by this key and reducer processes said groups.
  * @tparam W  transitional type -- used internally
  * @tparam V2 output type: the message which is sent on completion to the sender is of type Response[K2,V2]
  * @param config an instance of Config which defines a suitable configuration
  * @param f      the mapper function which takes a V1 and creates a key-value tuple of type (K2,W), wrapped in Try
  * @param g      the reducer function which combines two values (an V2 and a W) into one V2
  * @param z      the "zero" or "unit" (i.e. initializer) function which creates an "empty" V2.
  */
class Master_First_Fold[V1, K2, W, V2](config: Config, f: V1 => Try[(K2, W)], g: (V2, W) => V2, z: () => V2) extends MasterBaseFirst[V1, K2, W, V2](config, f, g, z) with ByFold[Unit, V1, K2, W, V2]

/**
  * @tparam K1 key type: the message which this actor responds to is of type Map[K1,V1].
  * @tparam V1 input type: the message which this actor responds to is of type Seq[V1].
  * @tparam K2 key type: mapper groups things by this key and reducer processes said groups.
  * @tparam W  transitional type -- used internally
  * @tparam V2 output type: the message which is sent on completion to the sender is of type Response[K2,V2]
  */
trait ByReduce[K1, V1, K2, W, V2 >: W] {
  /**
    * CONSIDER eliminating this method and its trait
    *
    * @param g the reduce function
    * @param z ignored
    * @return a Props instance
    */
  def reducerProps(g: (V2, W) => V2, z: () => V2): Props = Props.create(classOf[Reducer[K2, W, V2]], g)
}

/**
  * @tparam K1 key type: the message which this actor responds to is of type Map[K1,V1].
  * @tparam V1 input type: the message which this actor responds to is of type Seq[V1].
  * @tparam K2 key type: mapper groups things by this key and reducer processes said groups.
  * @tparam W  transitional type -- used internally
  * @tparam V2 output type: the message which is sent on completion to the sender is of type Response[K2,V2]
  */
trait ByFold[K1, V1, K2, W, V2] {
  /**
    * @param g the reducer function
    * @param z the "zero" or "unit" (i.e. initializer) function which creates an "empty" V2.
    * @return
    */
  def reducerProps(g: (V2, W) => V2, z: () => V2): Props = Props.create(classOf[Reducer_Fold[K2, W, V2]], g, z)
}

/**
  * Abstract class MasterBaseFirst
  *
  * This version of the MasterBase class (which it extends) take a different type of message: to wit, a Seq[V1].
  * That is to say, there is no K1 type.
  *
  * @tparam V1 input type: the message which this actor responds to is of type Seq[V1].
  * @tparam K2 key type: mapper groups things by this key and reducer processes said groups.
  * @tparam W  transitional type -- used internally
  * @tparam V2 output type: the message which is sent on completion to the sender is of type Response[K2,V2]
  * @param config an instance of Config which defines a suitable configuration
  * @param f      the mapper function which takes a V1 and creates a key-value tuple of type (K2,W), wrapped in Try
  * @param g      the reducer function which combines two values (an V2 and a W) into one V2
  * @param z      the "zero" or "unit" (i.e. initializer) function which creates an "empty" V2.
  */
abstract class MasterBaseFirst[V1, K2, W, V2](config: Config, f: V1 => Try[(K2, W)], g: (V2, W) => V2, z: () => V2) extends MasterBase[Unit, V1, K2, W, V2](config, Master.unitize(f), g, z) {

  import context.dispatcher

  override def receive: PartialFunction[Any, Unit] = {
    case v1s: Seq[V1] =>
      log.info(s"Master received Seq[V1]: with ${v1s.length} elements")
      val caller = sender // XXX: this looks strange but it is required
      doMapReduce(KeyValueSeq.sequence[Unit, V1](v1s)).onComplete {
        case Success(wXeK2m) => caller ! Response.create(wXeK2m)
        case Failure(x) => caller ! akka.actor.Status.Failure(x)
      }
    case q =>
      super.receive(q)
  }
}

/**
  * Note that logging the actual values received in the incoming message and other places can be VERY verbose.
  * It is therefore recommended practice to log the values as they pass through the mapper/reducer functions (f,g) which are
  * under the control of the application.
  * Therefore the various calls to maybeLog are commented out.
  *
  * @tparam K1 key type: the message which this actor responds to is of type Map[K1,V1].
  * @tparam V1 input type: the message which this actor responds to is of type Seq[V1].
  * @tparam K2 key type: mapper groups things by this key and reducer processes said groups.
  * @tparam W  transitional type -- used internally
  * @tparam V2 output type: the message which is sent on completion to the sender is of type Response[K2,V2]
  * @param config an instance of Config which defines a suitable configuration
  * @param f      the mapper function which takes a K1,V1 pair and creates a key-value tuple of type (K2,W), wrapped in Try
  * @param g      the reducer function which combines two values (an V2 and a W) into one V2
  * @param z      the "zero" or "unit" (i.e. initializer) function which creates an "empty" V2.
  */
abstract class MasterBase[K1, V1, K2, W, V2](config: Config, f: (K1, V1) => Try[(K2, W)], g: (V2, W) => V2, z: () => V2) extends MapReduceActor {

  // CONSIDER using Using
  private val actors = Actors(context.system, config)

  implicit val timeout: Timeout = getTimeout(config.getString("timeout"))

  log.debug(s"MasterBase: timeout=$timeout")

  import context.dispatcher

  // NOTE: the mapper and reducers should be terminated when this master is terminated.
  private val mapper = actors.createActor(context, Some(Master.sMpr), mapperProps)
  private val nReducers = config.getInt("reducers")
  log.debug(s"creating $nReducers reducers")
  private val reducers = for (i <- 1 to nReducers) yield
    actors.createActor(context, Some(s"${Master.sReducer}-$i"), reducerProps(g, z))
  if (Master.isForgiving(config)) log.debug("setting forgiving mode")

  /**
    * @return an instance of Props appropriate to the the given parameters
    */
  def mapperProps: Props =
    if (Master.isForgiving(config)) Props.create(classOf[Mapper_Forgiving[K1, V1, K2, W]], f) else Props.create(classOf[Mapper[K1, V1, K2, W]], f)

  /**
    * @param g the reducer function which combines two values (an V2 and a W) into one V2
    * @param z the "zero" or "unit" (i.e. initializer) function which creates an "empty" V2.
    * @return an instance of Props appropriate to the the given parameters
    */
  def reducerProps(g: (V2, W) => V2, z: () => V2): Props

  // CONSIDER reworking this so that there is only one possible valid message:
  // either in Map[] form of Seq[()] form. I don't really like having both
  override def receive: PartialFunction[Any, Unit] = {
    case v1K1m: Map[K1, V1] =>
      log.info(s"Master received Map[K1,V1]: with ${v1K1m.size} elements")
      //      maybeLog("received: {}",v1K1m)
      val caller = sender
      doMapReduce(KeyValueSeq.map[K1, V1](v1K1m)).onComplete {
        case Success(v2XeK2m) =>
          maybeLog("response: {}", v2XeK2m)
          caller ! Response.create(v2XeK2m)
        case Failure(x) =>
          log.error(x, s"no response--failure")
          caller ! akka.actor.Status.Failure(x)
      }
    case v1s: Seq[(K1, V1)]@unchecked =>
      log.info(s"Master received Seq[(K1,V1)]: with ${v1s.length} elements")
      //      maybeLog("received: {}",v1s)
      val caller = sender
      doMapReduce(KeyValueSeq[K1, V1](v1s)).onComplete {
        case Success(v2XeK2m) => caller ! Response.create(v2XeK2m)
        case Failure(x) => caller ! akka.actor.Status.Failure(x)
      }
    case q =>
      super.receive(q)
  }

  def doMapReduce(i: KeyValueSeq[K1, V1]): Future[Map[K2, Either[Throwable, V2]]] = for {
    wsK2m <- doMap(i)
    v2XeK2m <- doDistributeReduceCollate(wsK2m)
  } yield v2XeK2m


  override def close(): Unit = {
    actors.close()
    super.close()
  }

  private def doMap(i: KeyValueSeq[K1, V1]): Future[Map[K2, Seq[W]]] = {
    // NOTE this involves a cast to the parametric type Z which can result in a ClassCastException
    def iToMapper[Z: ClassTag]: Future[Z] = (mapper ? i).mapTo[Z]

    iToMapper[(Map[K2, Seq[W]], Seq[Throwable])] flatMap {
      case (m, xs) =>
        if (xs.nonEmpty && !Master.isForgiving(config)) Future.failed[Map[K2, Seq[W]]](xs.head)
        else {
          xs.foreach(logException)
          Future.successful(m)
        }
    }
  }

  private def doDistributeReduceCollate(wsK2m: Map[K2, Seq[W]]): Future[Map[K2, Either[Throwable, V2]]] = {
    if (wsK2m.isEmpty) log.warning("mapper returned empty map" + (if (Master.isForgiving(config: Config)) "" else ": see log for problem and consider using Mapper_Forgiving instead"))
    val v2XeK2fs = for (((k2, ws), a) <- distributeWork(wsK2m)) yield doReductionAsync(k2, ws, a)
    // TODO Where are we getting a null from?
    for (wXeK2s <- Future.sequence(v2XeK2fs)) yield wXeK2s.toMap
  }

  // NOTE this involves a cast to the parametric type (K2, Either[Throwable, V2]) which can result in a ClassCastException
  private def doReductionAsync(k2: K2, ws: Seq[W], actor: ActorRef): Future[(K2, Either[Throwable, V2])] = (actor ? Intermediate(k2, ws)).mapTo[(K2, Either[Throwable, V2])]

  // NOTE that this method operates in real time, without the protection of Try
  private def distributeWork(wsK2m: Map[K2, Seq[W]]): Seq[((K2, Seq[W]), ActorRef)] = {
    val rs = LazyList.continually(reducers.to(LazyList)).flatten
    val wsK2s = for ((k2, ws) <- wsK2m.to(Seq)) yield (k2, ws)
    wsK2s zip rs
  }

  private def logException(x: Throwable): Unit = actors.logException("mapper exception", x)

  //    if (exceptionStack) log.error(x, "mapper exception") else log.warning("mapper exception {}", x.getLocalizedMessage)
}

case class Response[K, V](left: Map[K, Throwable], right: Map[K, V]) {
  override def toString = s"left: $left; right: $right"

  def size: Int = right.size
}

object Response {

  import FP._

  def create[K, V](vXeKm: Map[K, Either[Throwable, V]]): Response[K, V] =
    invokeTupled(toMap(sequenceLeftRight(vXeKm)))(apply)
}

object Master {
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
  def unitize[A, B](f: A => B): (Unit, A) => B = {
    (_, v) => f(v)
  }

  //noinspection SpellCheckingInspection
  val sReducer = "rdcr"
  val sMpr = "mpr"
}
