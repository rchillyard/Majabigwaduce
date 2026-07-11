/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce.core

import akka.actor.typed.scaladsl.AskPattern.*
import akka.actor.typed.{ActorRef, Behavior, Scheduler}
import akka.util.Timeout
import com.phasmid.majabigwaduce.{ASync, RF}

import scala.concurrent.*
import scala.util.Try

/**
 * MapReduce is a trait, with case classes, which implements a functional API for the map-reduce classes in this package.
 *
 * @author scalaprof
 * @tparam T  the input type of the MapReduce function: T may be V1 for a first stage, or (K1,V1) for a subsequent stage.
 * @tparam K1 the key type of the returned map
 * @tparam V1 the value type of the returned map
 */
trait MapReduce[T, K1, V1] extends ASync[Seq[T], Map[K1, V1]] with AutoCloseable:

  self =>

  /**
   * Compose this MapReduce object with mr, yielding a new MapReduce object.
   *
   * @tparam K2 the key type of the composed MapReduce object
   * @tparam V2 the value type of the composed MapReduce object
   * @param f a function of type ASync[Seq[(K1, V1)], Map[K2, V2], i.e. Seq[(K1, V1)]=>Future[Map[K2, V2]
   * @return a new MapReduceComposed object
   */
  def :&[K2, V2](f: ASync[Seq[(K1, V1)], Map[K2, V2]]): MapReduce[T, K2, V2] =
    MapReduceComposed(self, f)(self.ec)

  /**
   * Alternative formulation for compose method (:&)
   *
   * @param mr the other MapReduce object
   * @tparam K2 the key type of the composed MapReduce object
   * @tparam V2 the value type of the composed MapReduce object
   * @return a new MapReduceComposed object
   */
  def &[K2, V2](mr: MapReduce[(K1, V1), K2, V2]): MapReduce[T, K2, V2] = :&(mr)

  /**
   * terminate this MapReduce object with r, a reducer which yields a simple value
   *
   * @param r                the Reduce object
   * @param executionContext (implicit)
   * @tparam S the return type, which is a super-class of V1 (for sum, or sigma)
   * @return an Async function of Seq[T]=>Future[S] type S.
   */
  def :|[S](r: RF[K1, V1, S])(implicit executionContext: ExecutionContext): ASync[Seq[T], S] =
    ts => for (v2K2m <- self(ts); s = r(v2K2m)) yield s

  /**
   * alternative name to terminate
   *
   * @param r                the Reduce object
   * @param executionContext (implicit)
   * @tparam S the return type, which is a super-class of V1 (for sum, or sigma)
   * @return an Async function of Seq[T]=>Future[S] type S.
   */
  def |[S](r: RF[K1, V1, S])(implicit executionContext: ExecutionContext): ASync[Seq[T], S] =
    :|(r)(executionContext)

  /**
   * @return a suitable execution context
   */
  def ec: ExecutionContext

/**
 * A first-stage MapReduce class where the result type V1 is a super-type of the intermediate type W
 *
 * @tparam V0 input value type
 * @tparam K1 output key type
 * @tparam W  intermediate type
 * @tparam V1 output value type (super-type of W)
 * @param f       the mapper function which takes a V0 instance and creates a key-value tuple of type (K1,W)
 * @param g       the reducer function which combines two values (an V1 and a W) into one V1
 * @param actors  an instance of Actors
 * @param timeout the value of timeout to be used
 */
case class MapReduceFirst[V0, K1, W, V1 >: W](f: V0 => Try[(K1, W)], g: (V1, W) => V1)(actors: Actors, timeout: Timeout) extends MapReduce_LoggingBase[V0, Unit, V0, K1, V1](actors)(timeout):
  /**
   * @return a Behavior based on a new instance of Master_First
   */
  def createBehavior: Behavior[MasterCommand[Unit, V0, K1, V1]] = Master_First(actors.config, f, g)

  def toMasterPair(t: V0): (Unit, V0) = () -> t

  /**
   * Provides the name associated with the MapReduceFirst instance.
   *
   * @return An optional string containing the name "mrf-mstr", which is a static identifier
   *         tied to the `MapReduceFirst` implementation.
   */
  override def createName: Option[String] = Some(MapReduceFirst.sMrfMstr)

/**
 * Companion object for the `MapReduceFirst` class that provides utility methods relevant to the `MapReduceFirst` implementation.
 */
object MapReduceFirst:
  // The following apply method allows for a f which needs to be lifted to T=>Try[R]
  def create[V0, K1, W, V1 >: W](fy: V0 => (K1, W), g: (V1, W) => V1)(implicit actors: Actors, timeout: Timeout): MapReduceFirst[V0, K1, W, V1] =
    apply(FP.lift(fy), g)(actors, timeout)

  //noinspection SpellCheckingInspection
  val sMrfMstr: String = "mrf-mstr"

/**
 * A later-stage MapReduce class where the result type V1 is a super-type of the intermediate type W
 *
 * @tparam K0 input key type
 * @tparam V0 input value type
 * @tparam K1 output key type
 * @tparam W  intermediate type
 * @tparam V1 output value type (super-type of W)
 * @param f       the mapper function which takes a K0,V0 pair and creates a key-value tuple of type (K1,W)
 * @param g       the reducer function which combines two values (an V1 and a W) into one V1
 * @param n       the stage number of this map-reduce stage.
 * @param actors  an instance of Actors
 * @param timeout the value of timeout to be used
 */
case class MapReducePipe[K0, V0, K1, W, V1 >: W](f: (K0, V0) => Try[(K1, W)], g: (V1, W) => V1, n: Int)(implicit actors: Actors, timeout: Timeout) extends MapReduce_LoggingBase[(K0, V0), K0, V0, K1, V1](actors)(timeout):
  /**
   * Creates a Behavior for the Master actor, initialized with the configuration,
   * mapper function, and reducer function provided by the enclosing MapReducePipe.
   *
   * @return a Behavior configured for creating a Master actor
   */
  def createBehavior: Behavior[MasterCommand[K0, V0, K1, V1]] = Master(actors.config, f, g)

  def toMasterPair(t: (K0, V0)): (K0, V0) = t

  /**
   * Generates a name for the master actor of the current map-reduce stage.
   *
   * The name is constructed using a predefined prefix and the stage number
   * associated with this MapReducePipe instance.
   *
   * @return an Option containing the generated name as a String
   */
  override def createName: Option[String] = Some(s"""${MapReducePipe.sMrpMaster}-$n""")

/**
 * Companion object for the `MapReducePipe` case class, providing utility functions
 * for creating instances of the `MapReducePipe`.
 */
object MapReducePipe:
  /**
   * Creates a `MapReducePipe` instance by lifting the provided mapper function into a form that returns a `Try` result.
   * The following apply method allows for a `f` which needs to be lifted to `T=>Try[R]`.
   *
   * @param f       a mapper function that takes a key-value pair of types `(K0, V0)` and produces a new key-value pair of types `(K1, W)`
   * @param g       a reducer function that combines two values, one of type `V1` (super-type of `W`) and the other of type `W`, into a single value of type `V1`
   * @param n       the stage number for the map-reduce process
   * @param actors  an implicit `Actors` instance to manage the underlying actor infrastructure
   * @param timeout an implicit `Timeout` specifying the maximum duration for actor operations
   * @return a `MapReducePipe` instance configured with the lifted mapper and reducer functions for the specified stage
   */
  def create[K0, V0, K1, W, V1 >: W](f: (K0, V0) => (K1, W), g: (V1, W) => V1, n: Int)(implicit actors: Actors, timeout: Timeout): MapReducePipe[K0, V0, K1, W, V1] =
    apply(FP.lift(f), g, n)(actors, timeout)

  //noinspection SpellCheckingInspection
  private val sMrpMaster: String = "mrf-mstr"


/**
 * A first-stage MapReduce class
 *
 * @tparam V0 input value type.
 * @tparam K1 output key type.
 * @tparam W  intermediate type.
 * @tparam V1 output value type (must support type class Init).
 * @param f       the mapper function which takes a V0 instance and creates a key-value tuple of type (K1,W) (wrapped in Try, but see alternative constructor).
 * @param g       the reducer function which combines two values (an V1 and a W) into one V1.
 * @param actors  an instance of Actors.
 * @param timeout the value of timeout to be used.
 *
 *                CONSIDER why is config parameter set not implicit?
 */
//noinspection SpellCheckingInspection
case class MapReduceFirstFold[V0, K1, W, V1: Zero](f: V0 => Try[(K1, W)], g: (V1, W) => V1)(actors: Actors, timeout: Timeout) extends MapReduce_LoggingBase[V0, Unit, V0, K1, V1](actors)(timeout):
  // The following constructor allows for a f which needs to be lifted to T=>Try[R]
  // CONSIDER implementing an apply method in MapReduce for this signature
  //  def this(fy: V0 => (K1, W), g: (V1, W) => V1)(actors: Actors, timeout: Timeout) = this(MapReduce.lift(fy), g)(actors, timeout)
  def createBehavior: Behavior[MasterCommand[Unit, V0, K1, V1]] =
    Master_First_Fold(actors.config, f, g, () => summon[Zero[V1]].zero)

  def toMasterPair(t: V0): (Unit, V0) = () -> t

  /**
   * Provides the name of the MapReduce master actor used in the first-stage fold operation.
   *
   * @return An `Option[String]` containing the name of the MapReduce master actor, if defined.
   */
  override def createName: Option[String] = Some(MapReduceFirstFold.sMrffMstr)

/**
 * Companion object for `MapReduceFirstFold`, providing factory methods and constants.
 */
object MapReduceFirstFold:
  /**
   * Creates a `MapReduceFirstFold` instance using the given mapper function, reducer function, actor configuration, and timeout.
   *
   * @param f       the mapper function that transforms an input of type `V0` into a tuple `(K1, W)`.
   * @param g       the reducer function that combines a value of type `V1` with a value of type `W` into a new value of type `V1`.
   * @param actors  the actor configuration used to manage the underlying actors.
   * @param timeout the timeout value that determines the maximum time to wait for an operation to complete.
   * @return a `MapReduceFirstFold` instance parameterized with types `V0`, `K1`, `W`, and `V1`.
   */
  def create[V0, K1, W, V1: Zero](f: V0 => (K1, W), g: (V1, W) => V1)(actors: Actors, timeout: Timeout): MapReduceFirstFold[V0, K1, W, V1] =
    apply(FP.lift(f), g)(actors, timeout)

  //noinspection SpellCheckingInspection
  private val sMrffMstr = "mrff-mstr"

/**
 * A later-stage MapReduce class
 *
 * @tparam K0 input key type
 * @tparam V0 input value type
 * @tparam K1 output key type
 * @tparam W  intermediate type
 * @tparam V1 output value type (must support type class Init)
 * @param f       the mapper function which takes a V0 instance and creates a key-value tuple of type (K1,W)
 * @param g       the reducer function which combines two values (an V1 and a W) into one V1
 * @param n       the stage number of this map-reduce stage.
 * @param actors  an instance of Actors.
 * @param timeout the value of timeout to be used
 */
case class MapReducePipeFold[K0, V0, K1, W, V1: Zero](f: (K0, V0) => Try[(K1, W)], g: (V1, W) => V1, n: Int)(actors: Actors, timeout: Timeout) extends MapReduce_LoggingBase[(K0, V0), K0, V0, K1, V1](actors)(timeout):
  /**
   * Creates and returns a `Behavior` for the `Master_Fold` actor.
   * Encapsulates the actor's configuration, including the mapper function `f`, reducer function `g`, and
   * an initializer function to generate a zero value of type V1.
   *
   * The following constructor allows for a f which needs to be lifted to T=>Try[R]
   * def this(fy: (K0, V0) => (K1, W), g: (V1, W) => V1, n: Int)(actors: Actors, timeout: Timeout) = this(MapReduce.lift(fy), g, n)(actors, timeout)
   *
   * @return a `Behavior` instance for creating the `Master_Fold` actor.
   */
  def createBehavior: Behavior[MasterCommand[K0, V0, K1, V1]] =
    Master_Fold(actors.config, f, g, () => summon[Zero[V1]].zero)

  def toMasterPair(t: (K0, V0)): (K0, V0) = t

  /**
   * Generates a name for the current MapReduce pipe fold stage.
   * The generated name includes a prefix and the stage number.
   *
   * @return An `Option` containing the generated name as a `String`,
   *         or `None` if the name could not be generated.
   */
  override def createName: Option[String] =
    Some(s"""${MapReducePipeFold.sMRPFMaster}-$n""")

/**
 * Singleton object for creating and managing `MapReducePipeFold` instances.
 *
 * This companion object provides utility methods to initialize `MapReducePipeFold`
 * instances, given appropriate functions for mapping and reducing, stage numbers,
 * actor system configurations, and timeout settings.
 */
object MapReducePipeFold:
  /**
   * Creates a MapReducePipeFold instance by applying the provided mapper and reducer functions.
   *
   * @param f       the mapper function that transforms an input key-value pair of type (K0, V0)
   *                into an intermediate key-value pair of type (K1, W)
   * @param g       the reducer function that combines a value of type V1 with a value of type W
   *                to produce a new value of type V1
   * @param n       the stage number for this map-reduce operation
   * @param actors  an instance of Actors used for managing the actor system
   * @param timeout the timeout configuration for the operation
   * @return a new instance of MapReducePipeFold configured with the given functions and parameters
   */
  def create[K0, V0, K1, W, V1: Zero](f: (K0, V0) => (K1, W), g: (V1, W) => V1, n: Int)(actors: Actors, timeout: Timeout): MapReducePipeFold[K0, V0, K1, W, V1] =
    apply(FP.lift(f), g, n)(actors, timeout)

  //noinspection SpellCheckingInspection
  private val sMRPFMaster = "mrpf-mstr"

/**
 * A composition MapReduce class which represents a MapReduce "pipeline" with two stages.
 *
 * @tparam T  the input type of the MapReduce function: T may be V1 for a first stage, or (K1,V1) for a subsequent stage.
 * @tparam K1 intermediate key type
 * @tparam V1 intermediate value type
 * @tparam K2 output key type
 * @tparam V2 output value type
 * @param f1 an instance of MapReduce which will become the first of two stages of the resulting MapReduce instance .
 * @param f2 an ASync[Seq[(K1, V1)], Map[K2, V2] instance (functionally equivalent to MapReduce) which will become the second of the two stages.
 */
case class MapReduceComposed[T, K1, V1, K2, V2](f1: MapReduce[T, K1, V1], f2: ASync[Seq[(K1, V1)], Map[K2, V2]])(implicit val ec: ExecutionContext) extends MapReduce[T, K2, V2]:
  /**
   * Applies the MapReduce pipeline to the provided input sequence, transforming it through
   * the first stage and then the second stage to produce the final output.
   *
   * @param ts the input sequence of type T to be processed through the MapReduce pipeline
   * @return a Future containing a map of type Map[K2, V2], which represents the transformed output
   */
  def apply(ts: Seq[T]): Future[Map[K2, V2]] =
    for v2K2m: Map[K1, V1] <- f1(ts); v3K3m: Map[K2, V2] <- f2(v2K2m.toSeq) yield v3K3m

  /**
   * Closes the first stage of the MapReduce pipeline, releasing any resources associated with it.
   *
   * @return Unit, indicating that the operation completes without returning a value.
   */
  def close(): Unit = f1.close()

/**
 * A reduce function which can be composed (on the right) with a MapReduce object.
 *
 * @param f the function which will combine the current result with each element of an input set
 * @tparam T the input (free) type of this reduction
 * @tparam S the output (derived) type of this reduction
 */
case class Reduce[K, T, S: Zero](f: (S, T) => S) extends RF[K, T, S]:
  /**
   * This method cannot use reduce because, logically, reduce is not able to process an empty collection.
   * Note that we ignore the keys of the input map (m)
   *
   * @param m the input map (keys will be ignored)
   * @return the result of combining all values of m, using the f function.
   *         An empty map will result in the value of z() being returned.
   */
  def apply(m: Map[K, T]): S =
    m.values.foldLeft(summon[Zero[S]].zero)(f)

/**
 * An abstract base class which extends MapReduce_Base and which implements the logException method with non-trivial logging.
 *
 * @tparam T   the input type of the MapReduce function: T may be V1 for a first stage, or (K1,V1) for a subsequent stage.
 * @tparam K1M the key type of the master's own protocol (Unit for a first stage, or the outer K1 for a subsequent stage).
 * @tparam V1M the value type of the master's own protocol.
 * @tparam K1  intermediate key type
 * @tparam V1  intermediate value type
 * @param actors  an instance of Actors
 * @param timeout the value of timeout to be used
 */
abstract class MapReduce_LoggingBase[T, K1M, V1M, K1, V1](actors: Actors)(timeout: Timeout) extends MapReduce_Base[T, K1M, V1M, K1, V1](actors)(using timeout):
  /**
   * Logs an exception using the provided message and throwable.
   *
   * @param m the message to be logged; this is a lazy parameter and will only be evaluated if needed
   * @param x the exception to be logged alongside the message
   * @return Unit, as this method performs a logging side effect and does not return a value
   */
  def logException(m: => String, x: Throwable): Unit =
    actors.logException(m, x)

/**
 * An abstract base class for MapReduce classes (other than MapReduceComposed).
 *
 * @tparam T   the input type of the MapReduce function: T may be V1 for a first stage, or (K1,V1) for a subsequent stage.
 * @tparam K1M the key type of the master's own protocol (Unit for a first stage, or K for a subsequent stage).
 * @tparam V1M the value type of the master's own protocol (T for a first stage, or the V1 half of T for a subsequent stage).
 * @tparam K   output key type
 * @tparam V   output value type
 */
abstract class MapReduce_Base[T, K1M, V1M, K, V](actors: Actors)(using timeout: Timeout) extends MapReduce[T, K, V]:
  given ec: ExecutionContext = actors.system.executionContext

  private given scheduler: Scheduler = actors.system.scheduler

  private val master: ActorRef[MasterCommand[K1M, V1M, K, V]] =
    actors.createActor[MasterCommand[K1M, V1M, K, V]]((b, n) => actors.system.systemActorOf(b, n), createName, createBehavior)

  /**
   * Processes a sequence of input items, communicates with the master actor to retrieve results,
   * and optionally reports the response.
   *
   * @param ts the sequence of input items to be processed
   * @return a Future containing a map of key-value pairs representing the computed results
   */
  def apply(ts: Seq[T]): Future[Map[K, V]] =
    // Note: currently, we ignore the value of report but we could pass back a tuple that includes ok and the resulting map
    for
      t <- master.ask[Try[Response[K, V]]](replyTo => ComputeSeq(ts.map(toMasterPair), replyTo))
      vKr <- Future.fromTry(t)
      _ = report(vKr)
    yield vKr.right

  /**
   * Converts an input item of type T into the (key, value) pair the master's own protocol expects.
   * For a first-stage MapReduce, this pairs T with a Unit key; for a subsequent stage, T already is
   * the (K,V) pair the master expects.
   */
  def toMasterPair(t: T): (K1M, V1M)

  /**
   * Creates and returns the Behavior for the master actor.
   *
   * @return the Behavior used for actor creation
   */
  def createBehavior: Behavior[MasterCommand[K1M, V1M, K, V]]

  /**
   * This probably ought to be configured according to whether or not we are debugging
   *
   * @return
   */
  def createName: Option[String] = None

  /**
   * Logs the provided exception with an accompanying message.
   *
   * @param m a lazily evaluated message string to provide context for the exception
   * @param x the exception to be logged
   * @return Unit, as the method performs a logging operation and does not return a value
   */
  def logException(m: => String, x: Throwable): Unit

  /**
   * Stops the master actor (and, transitively, its mapper and reducer children) to release
   * resources and terminate associated processes.
   *
   * @return Unit, as the method performs an action and does not produce a value.
   */
  def close(): Unit =
    master ! CloseMaster()

  /**
   * Logs exceptions associated with the left entries of the provided response and evaluates
   * whether the response contains any successful entries on the right.
   *
   * @param vKr the response object containing a map of exceptions (left) and successful results (right)
   * @return true if the response contains no successful entries in its right map, false otherwise
   */
  private def report(vKr: Response[K, V]): Boolean =
    for ((k, x) <- vKr.left) logException(s"exception thrown (but forgiven) for key $k", x)
    vKr.size == 0
