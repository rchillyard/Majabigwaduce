/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce.dd

import akka.actor.ActorSystem
import akka.util.Timeout
import com.phasmid.majabigwaduce.core.{Actors, MapReducePipe, Monoid, Zero}
import com.phasmid.majabigwaduce.dd.DataDefinition.IterableMonoid
import com.phasmid.majabigwaduce.dd.LazyDD.joinMap
import com.phasmidsoftware.flog.{Flog, Loggable}
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext, Future}

/**
 * Trait to represent a "data definition" (similar to RDD in Spark).
 * Essentially, a DataDefinition[K, V] is a function which transforms Unit into a Future[Map[K,V].
 * Like RDD, it is lazy and can be partitioned.
 * In order to yield a concrete value, i.e. an "action", there are three methods which may be called: apply, reduce(f), and count.
 *
 * Created by scalaprof on 10/31/16.
 *
 * @tparam K the key type
 * @tparam V the value type
 */
sealed trait DataDefinition[K, V] extends (() => Future[Map[K, V]]):

  /**
   * Method to form a new DataDefinition where the resulting values derive from applying the function f to the original values
   *
   * @param f the function to transform key-value pairs
   * @tparam L the underlying type of the keys of the resulting map
   * @tparam W the underlying type of the values of the resulting map
   * @return a new DataDefinition
   */
  def map[L, W: Monoid](f: ((K, V)) => (L, W)): DataDefinition[L, W]

  /**
   * Method to evaluate this DataDefinition and reduce the dimensionality of the result by ignoring the keys
   * and aggregating the values according to the function wv_w
   *
   * @param wVWf the aggregation function
   * @tparam W the underlying type of the result
   * @return a W value, wrapped in Future.
   */
  def reduce[W: Zero](wVWf: (W, V) => W): Future[W]

  /**
   * Evaluate the number of elements in this DataDefinition
   *
   * @return the number of k-v pairs
   */
  def count: Future[Int]

  /**
   * Method to filter this DataDefinition according to a predicate which takes a k-v tuple.
   *
   * @param p the predicate which will yield a Boolean for a given k-v tuple.
   * @return a new DataDefinition containing only those k-v pairs which satisfy the predicate p.
   */
  def filter(p: ((K, V)) => Boolean): DataDefinition[K, V]

  /**
   * Method to group values by a new key type generated from the values, ignoring the current keys.
   *
   * @tparam L the new key type, whose values are derived from the values of this DataDefinition
   * @return a DataDefinition based on L and Iterable[V]
   */
  def groupBy[L](f: V => L): DataDefinition[L, Iterable[V]]

  /**
   * Join method to perform inner join.
   *
   * @param other the DataDefinition with which to join this
   * @tparam L key type of other and also of the result
   * @tparam W value type of other
   * @return the inner join of this and other
   */
  def join[L >: K, W: Monoid](other: DataDefinition[L, W]): DataDefinition[L, (V, W)]

  /**
   * Clean up any residual resources from this DataDefinition
   */
  def clean(): Unit

/**
 * Case Class which implements DataDefinition[K, V] eagerly and which is based on a Map[K,V].
 * NOTE: this type serves as the appropriate result of evaluating a lazy DD.
 * In that respect it is the equivalent of the array which is created when invoking collect on an RDD in Spark.
 *
 * @param kVs the actual data definition represented as a Map
 * @param ec  the (implicit) execution context
 * @tparam K the key type
 * @tparam V the input value type
 */
case class EagerDD[K, V](kVs: Seq[(K, V)])(implicit ec: ExecutionContext) extends BaseDD[K, V] with HasEvaluatedMap[K, V]:

  /**
   * Method to form a new DataDefinition where the resulting values derive from applying the function f to the original values
   *
   * @param f the function to transform key-value pairs
   * @tparam L the underlying type of the keys of the resulting map
   * @tparam W the underlying type of the values of the resulting map
   * @return a new DataDefinition
   */
  def map[L, W: Monoid](f: ((K, V)) => (L, W)): DataDefinition[L, W] = EagerDD(for ((k, v) <- kVs) yield f(k, v))

  /**
   * Method to filter this DataDefinition according to a predicate which takes a k-v tuple.
   *
   * @param p the predicate which will yield a Boolean for a given k-v tuple.
   * @return a new DataDefinition containing only those k-v pairs which satisfy the predicate p.
   */
  def filter(p: ((K, V)) => Boolean): DataDefinition[K, V] = EagerDD[K, V](kVs.filter(p))

  /**
   * Join method to perform inner join.
   *
   * @param other the DataDefinition with which to join this
   * @tparam L key type of other and also of the result
   * @tparam W value type of other
   * @return the inner join of this and other
   */
  def join[L >: K, W: Monoid](other: DataDefinition[L, W]): DataDefinition[L, (V, W)] = other match {
    case edd: EagerDD[L, W] @unchecked => EagerDD[L, (V, W)](joinMap(kVs.toMap.asInstanceOf[Map[L, V]], edd.kVs.toMap).toSeq)
    case bdd: BaseDD[L, W] @unchecked =>
      import scala.concurrent.duration.*
      given timeout: Timeout = Timeout(5.seconds)

      join(Await.result(bdd.evaluate, timeout.duration))
    case _ => throw DataDefinitionException("join not supported for Eager and non-Eager DataDefinition objects")
  }

  /**
   * Return the evaluated map as is
   *
   * @return a Map[K,V]
   */
  def evalMap: Map[K, V] = kVs.toMap

  /**
   * Evaluate this EagerDD as a Future of DataDefinition[K,V] with HasEvaluatedMap[K, V]
   *
   * @return this wrapped in Future
   */
  def evaluate: Future[DataDefinition[K, V] with HasEvaluatedMap[K, V]] = Future(this)

  /**
   * Clean up any residual resources from this DataDefinition.
   * For an EagerDD, this is a no-op.
   */
  def clean(): Unit = ()

  /**
   * Method to group values by a new key type generated from the values, ignoring the current keys.
   *
   * CONSIDER simplifying the value passed to EagerDD
   *
   * @tparam L the new key type, whose values are derived from the values of this DataDefinition
   * @return a DataDefinition based on L and Iterable[V]
   */
  def groupBy[L](f: V => L): DataDefinition[L, Iterable[V]] = EagerDD(kVs.toMap.values.groupBy(f).toSeq)

/**
 * A data structure that eagerly evaluates and stores key-value pairs.
 *
 * `EagerDD` is designed for scenarios where key-value data needs to be stored 
 * and accessed in an immutable and thread-safe manner.
 */
object EagerDD:
  /**
   * Constructs an instance of `EagerDD` from the provided map of key-value pairs.
   *
   * @param kVs the map containing key-value pairs to be used for initialization
   * @param ec  the implicit execution context required for asynchronous computations
   * @return an `EagerDD` instance initialized with the provided key-value pairs
   */
  def apply[K, V](kVs: Map[K, V])(implicit ec: ExecutionContext): EagerDD[K, V] = EagerDD(kVs.toSeq)

/**
 * Case Class which implements DataDefinition[K, W] and which is based on a Map[K,V] and a function V => W.
 *
 * CONSIDER a property cutoff to determine whether it's appropriate to invoke the map-reduce machinery on kVs.
 *
 * @param kVs        the map of key-value pairs which serve as the input to this LazyDD
 * @param f          a function which will transform the key-value pairs
 * @param partitions the number of partitions to be used
 * @param context    a DDContext
 * @tparam K the key type
 * @tparam V the input value type
 * @tparam W the output value type
 */
case class LazyDD[K, V, L, W: Monoid]
(kVs: Seq[(K, V)], f: ((K, V)) => (L, W))
(partitions: Int = DataDefinition.DefaultPartitions)
(implicit context: DDContext)
  extends BaseDD[L, W]()(context.ec):

  private given cfs: Config = context.config

  private given sys: ActorSystem = context.system

  private given to: Timeout = context.timeout

  private given ec: ExecutionContext = context.ec

  private val flog = Flog(LazyDD.logger)

  import flog.*

  given kVloggable: Loggable[(K, V)] = (t: (K, V)) => s"${t._1} -> ${t._2}"

  s"created LazyDD with number of partitions" !? partitions
  s"... and with kVs" !? kVs

  /**
   * Method to form a new DataDefinition where the resulting values derive from applying the function f to the original values
   *
   * @param g the function to transform key-value pairs
   * @tparam Y the underlying type of the keys of the resulting map
   * @tparam X the underlying type of the values of the resulting map
   * @return a new DataDefinition
   */
  def map[Y, X: Monoid](g: ((L, W)) => (Y, X)): DataDefinition[Y, X] =
    LazyDD[K, V, Y, X](kVs, f andThen g)(partitions)

  /**
   * Method to filter this DataDefinition according to a predicate which takes a l-w tuple.
   *
   * @param p the predicate which will yield a Boolean for a given l-w tuple.
   * @return a new DataDefinition containing only those l-w pairs which satisfy the predicate p.
   */
  def filter(p: ((L, W)) => Boolean): DataDefinition[L, W] =
    LazyDD[K, V, L, W](kVs.filter(f andThen p), f)(partitions)

  /**
   * Join method to perform inner join.
   *
   * @param other the DataDefinition with which to join this
   * @tparam M key type of other and also of the result
   * @tparam X value type of other
   * @return the inner join of this and other
   */
  def join[M >: L, X: Monoid](other: DataDefinition[M, X]): DataDefinition[M, (W, X)] = other match
    case ldd: LazyDD[K, X, M, X] @unchecked =>
      import LazyDD.*
      LazyDD[K, (V, X), M, (W, X)](joinMap2(kVs.toMap, ldd.kVs.toMap, f).toSeq, joinFunction(f, ldd.f))(partitions)
    case edd: EagerDD[M, X] =>
      join(LazyDD[M, X, M, X](edd.kVs, identity)(partitions))
    case _ =>
      throw DataDefinitionException("join not supported for Lazy and Base DataDefinition objects")

  /**
   * Method to group values by a new key type generated from the values, ignoring the current keys.
   *
   * @tparam M the new key type, whose values are derived from the values of this DataDefinition
   * @return a DataDefinition based on M and Iterable[V]
   */
  def groupBy[M](f: W => M): DataDefinition[M, Iterable[W]] =
    implicit object IterableMonoidW extends IterableMonoid[W]
    // CONSIDER simplify this
    DataDefinition(applyFunction.toMap.values.groupBy(f).toSeq)

  /**
   * Evaluate this LazyDD as a Future of DataDefinition[L,W] with HasEvaluatedMap[L,W]
   *
   * @return an EagerDD[L,W] wrapped in Future
   */
  def evaluate: Future[DataDefinition[L, W] with HasEvaluatedMap[L, W]] =
    if partitions < 2
    then Future(EagerDD(applyFunction))(scala.concurrent.ExecutionContext.Implicits.global)
    else
      given actors: Actors = Actors(summon[ActorSystem], summon[Config])

      val mr = MapReducePipe.create[K, V, L, W, W]((k, v) => f((k, v)), summon[Monoid[W]].combine, 1)
      context.register(mr)
      for (x <- mr(kVs)) yield EagerDD(x.toSeq)

  /**
   * A helper method that applies the function `f` to each key-value pair in `kVs`.
   *
   * This method extracts elements from `kVs` sequentially, invokes the function `f`
   * with each key `k` and value `v`, and collects the resulting tuples into a sequence.
   *
   * @return a sequence of transformed key-value pairs as produced by applying `f`
   *         to each pair from the input collection `kVs`.
   */
  private def applyFunction: Seq[(L, W)] =
    for ((k, v) <- kVs) yield f(k, v)

  /**
   * Clean up any resources in the context of this LazyDD object
   */
  def clean(): Unit = context.clean()

/**
 * This trait is essentially a private trait: only to be used by this module.
 *
 * @tparam K the key type
 * @tparam V the value type
 */
sealed trait HasEvaluatedMap[K, V]:
  /**
   * Provides a map of key-value pairs that represents the evaluated elements.
   *
   * @return a map containing the evaluated key-value pairs
   */
  def evalMap: Map[K, V]

/**
 * Abstract base class which implements the generic DataDefinition[K, V].
 *
 * @param ec an ExecutionContext
 * @tparam K the key type
 * @tparam V the input value type
 */
abstract class BaseDD[K, V](implicit ec: ExecutionContext) extends DataDefinition[K, V]:

  /**
   * Evaluate this DataDefinition
   *
   * @return a map of key-value pairs wrapped in Future
   */
  override def apply(): Future[Map[K, V]] = evaluate map (_.evalMap)

  /**
   * Evaluate this BaseDD as a Future[HasEvaluatedMap[K, V]
   *
   * @return an HasEvaluatedMap (in practice, this will be an EagerDD) wrapped in Future
   */
  def evaluate: Future[DataDefinition[K, V] with HasEvaluatedMap[K, V]]

  /**
   * Method to evaluate this DataDefinition and reduce the dimensionality of the result by ignoring the keys
   * and aggregating the values according to the function xw_x.
   *
   * @param wVWf the aggregation function.
   * @tparam X the underlying type of the result.
   * @return an X value, wrapped in Future.
   */
  def reduce[X: Zero](wVWf: (X, V) => X): Future[X] =
    for kVm <- apply() yield kVm.values.foldLeft(summon[Zero[X]].zero)(wVWf)

  /**
   * Evaluate the number of elements in this DataDefinition
   *
   * @return the number of k-v pairs
   */
  def count: Future[Int] =
    for kVm <- apply() yield kVm.size

/**
 * The context in which DataDefinition instances will be evaluated
 *
 * @param config  the configuration
 * @param system  the actor system
 * @param timeout the value of timeout
 */
case class DDContext(config: Config, system: ActorSystem, timeout: Timeout)(implicit executor: ExecutionContext):
  // NOTE: consciously using var here.
  var closeables: List[AutoCloseable] = Nil

  /**
   * Cleans up resources by closing all registered closeables and resetting the collection of closeables.
   *
   * This method iterates over the registered closeables, invokes their `close` method, 
   * and then clears the list of closeables to ensure no further operations are performed on them.
   *
   * @return Unit
   */
  def clean(): Unit =
    for (closeable <- closeables) closeable.close()
    closeables = Nil

  /**
   * Registers one or more `AutoCloseable` resources to be managed.
   *
   * The registered resources will be added to the `closeables` list
   * for management and cleanup.
   *
   * @param cs varargs parameter representing the `AutoCloseable` resources to register
   * @return Unit since the method performs a side effect of updating the `closeables` list
   */
  def register(cs: AutoCloseable*): Unit =
    closeables = closeables ++ cs

  /**
   * Provides access to the execution context used by this instance of `DDContext`.
   *
   * This method allows components requiring a `scala.concurrent.ExecutionContext` to obtain it,
   * ensuring that asynchronous operations leverage the correct thread pool for execution.
   *
   * @return the `ExecutionContext` associated with this instance.
   */
  def ec: ExecutionContext = executor

  // TEST
  override def toString: String = s"DDContext: system=${system.name}, timeout=$timeout"

object DDContext:

  import java.util.concurrent.TimeUnit

  /**
   * Creates an instance of `DDContext` using the default configuration, actor system name, and timeout duration
   * specified in the application's configuration file. This method uses the implicit `ExecutionContext` for
   * managing asynchronous operations.
   *
   * @param executor an implicit parameter representing the `ExecutionContext` used for asynchronous tasks.
   * @return an instance of `DDContext` initialized with the configuration, actor system, and timeout settings.
   */
  def apply(implicit executor: ExecutionContext): DDContext =
    val config = ConfigFactory.load().getConfig("majabigwaduce.DataDefinition")
    val timeout = FiniteDuration(config.getDuration("timeout").getSeconds, TimeUnit.SECONDS)
    val system: ActorSystem = ActorSystem(config.getString("actorSystem"))
    apply(config, system, timeout)

/**
 * Represents a singleton object that provides definitions or metadata for data-related functionalities.
 *
 * This object is designed to encapsulate the structure and behavior associated with
 * defining, managing, or interpreting data schemas or specifications.
 *
 * It can be used to centralize data definitions, facilitate consistency across
 * data-related processes, and support extensible data handling operations.
 */
object DataDefinition:

  import scala.concurrent.ExecutionContext.Implicits.global

  given context: DDContext = DDContext.apply

  /**
   * Creates a lazy `DataDefinition` from a sequence of key-value pairs, partitioning the data into the specified
   * number of partitions. The provided sequence can contain duplicate keys, and the values associated with duplicate
   * keys will be combined using the `Monoid` instance for the value type.
   *
   * @param kVs        A sequence of key-value pairs to define the initial data.
   * @param partitions The number of partitions to split the data into. Each partition will contain roughly an equal
   *                   share of the data.
   * @tparam K The type of the keys in the key-value pairs.
   * @tparam V The type of the values in the key-value pairs. The values must form a monoid.
   * @return A `DataDefinition` representing the lazily computed data with the specified partitions.
   */
  def apply[K, V: Monoid](kVs: Seq[(K, V)], partitions: Int): DataDefinition[K, V] = LazyDD(kVs, identity[(K, V)])(partitions)

  /**
   * Creates a new `DataDefinition` from a sequence of key-value pairs, where the values
   * conform to the `Monoid` typeclass, enabling combination and aggregation operations.
   *
   * @param kVs the sequence of key-value pairs to initialize the `DataDefinition`
   * @tparam K the type of the keys in the sequence
   * @tparam V the type of the values in the sequence, which must have a `Monoid` implementation
   * @return a lazily evaluated `DataDefinition` based on the provided key-value sequence
   */
  def apply[K, V: Monoid](kVs: Seq[(K, V)]): DataDefinition[K, V] =
    LazyDD(kVs, identity[(K, V)])()

  /**
   * Partitions the given map into a distributed `DataDefinition` with a specified number of partitions.
   * The values in the map must adhere to the `Monoid` typeclass, allowing for combination and aggregation operations.
   *
   * @param kVm        the map containing key-value pairs to be partitioned
   * @param partitions the number of partitions for the resulting `DataDefinition`
   * @tparam K the type of the keys in the map
   * @tparam V the type of the values in the map, which must have a `Monoid` implementation
   * @return a lazily evaluated `DataDefinition` partitioned into the specified number of parts
   */
  def apply[K, V: Monoid](kVm: Map[K, V], partitions: Int): DataDefinition[K, V] =
    apply(kVm.toSeq)

  /**
   * Creates a `DataDefinition` from a map of key-value pairs, where the value type has a `Monoid` defined.
   *
   * @param kVm a map containing key-value pairs, where the value type has an associated `Monoid`.
   * @tparam K the type of the keys in the map.
   * @tparam V the type of the values in the map, which must have a `Monoid` instance.
   * @return a `DataDefinition` containing the provided key-value pairs.
   */
  def apply[K, V: Monoid](kVm: Map[K, V]): DataDefinition[K, V] =
    LazyDD(kVm.toSeq, identity[(K, V)])()

  /**
   * Transforms a sequence of values into a sequence of tuples where the first element
   * is a unit value and the second element is derived from the input sequence.
   *
   * @param vs A sequence of values of type V. The type V must have an implicit Monoid instance.
   * @return A sequence of tuples where each tuple contains a unit value as the first element
   *         and an element from the input sequence as the second element.
   */
  def expandTuples[V: Monoid](vs: Seq[V]): Seq[(Unit, V)] =
    LazyList.continually(()).zip(vs)

  /**
   * Method to create a LazyDD from a Seq[V], an function V => K, and a specific number of partitions.
   *
   * @param vs         a Seq[V].
   * @param f          the key generation function.
   * @param partitions the desired number of partitions.
   * @tparam K the key type.
   * @tparam V the underlying type of vs.
   * @return a LazyDD.
   */
  def apply[K, V: Monoid](vs: Seq[V], f: V => K, partitions: Int = DefaultPartitions): DataDefinition[K, V] =
    LazyDD[Any, V, K, V](expandTuples(vs), t => (f(t._2), t._2))(partitions)

  /**
   * Method to create a LazyDD from a sequence of V (no keys) and using the default partitions value.
   *
   * @param vs a Seq[V].
   * @param f  the key generation function.
   * @tparam K the key type.
   * @tparam V the underlying type of vs.
   * @return a LazyDD.
   */
  def create[K, V: Monoid](vs: Seq[V], f: V => K): DataDefinition[K, V] =
    apply(vs, f, DefaultPartitions)

  /**
   * This lift method is used to lift a V=>W into a (K,V)=>(K,W) and is used in those situations where only the values
   * of a key-value pair are to be transformed by the DataDefinition map method.
   *
   * @param f a V=>W function
   * @tparam K the key type
   * @tparam V the incoming value type
   * @tparam W the outgoing value type
   * @return a (K,V) => (K,W) function
   */
  def tupleLift[K, V, W](f: V => W): ((K, V)) => (K, W) = vToWToTupleToTuple(f)

  /**
   * A trait that defines a monoid for iterable collections of type `T`.
   *
   * Extends the `Monoid` trait, adding functionality for combining and initializing 
   * iterable collections. Provides a default implementation for the identity element
   * (`zero`) and the associative binary operation (`combine`).
   *
   * @tparam T the type of elements within the iterable collection.
   */
  trait IterableMonoid[T] extends Monoid[Iterable[T]]:
    def zero: Iterable[T] = Seq[T]()

    def combine(x: Iterable[T], y: Iterable[T]): Iterable[T] = x ++ y

  private def vToWToTupleToTuple[K, V, W](f: V => W)(t: (K, V)): (K, W) = (t._1, f(t._2))

  val DefaultPartitions: Int = 2

object LazyDD:
  private[majabigwaduce] def joinMap[K, V, W](map1: Map[K, V], map2: Map[K, W]): Map[K, (V, W)] =
    (for key <- map1.keySet intersect map2.keySet yield (key, (map1(key), map2(key)))).toMap

  private def joinFunction[K, V, L, W, X, Y](f: ((K, V)) => (L, W), g: ((K, X)) => (L, Y)): ((K, (V, X))) => (L, (W, Y)) =
    case (k, (v, x)) =>
      val vKf = f(k, v)
      vKf._1 -> (vKf._2, g(k, x)._2)

  private def joinMap2[K, V, L, W, X](map1: Map[K, V], map2: Map[K, W], f: ((K, V)) => (L, X)): Map[K, (V, W)] = {
    val commonKeys = map1.map(f).asInstanceOf[Map[K, V]].keySet intersect map2.keySet
    val validMap = map1.filter(x => commonKeys.contains(f.apply(x).asInstanceOf[(K, V)]._1))
    for ((k, v) <- validMap) yield (k, (map1(k), map2(f.apply(k, v).asInstanceOf[(K, V)]._1)))
  }

  val logger: Logger = LoggerFactory.getLogger(LazyDD.getClass)

case class DataDefinitionException(str: String) extends Exception(str)
