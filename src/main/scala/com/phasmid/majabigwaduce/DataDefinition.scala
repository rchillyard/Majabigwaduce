/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce

import akka.actor.ActorSystem
import akka.util.Timeout
import com.phasmid.majabigwaduce.DataDefinition.IterableMonoid
import com.phasmid.majabigwaduce.LazyDD.joinMap
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext, Future}

/**
  * Trait to represent a "data definition" (similar to RDD in Spark).
  * Essentially, a DataDefinition[K, V] is a function which transforms Unit into a Future[Map[K,V].
  * Like RDD, it is lazy and can be partitioned.
  * In order to yield a concrete value, i.e. an "action", there are three methods which may be called: apply(), reduce(f), and count.
  *
  * Created by scalaprof on 10/31/16.
  *
  * @tparam K the key type
  * @tparam V the value type
  */
sealed trait DataDefinition[K, V] extends (() => Future[Map[K, V]]) {

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
    * @param wv_w the aggregation function
    * @tparam W the underlying type of the result
    * @return a W value, wrapped in Future.
    */
  def reduce[W: Zero](wv_w: (W, V) => W): Future[W]

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
}

/**
  * Case Class which implements DataDefinition[K, V] eagerly and which is based on a Map[K,V].
  * NOTE: there is no very practical usage for this type, but it serves as an alternative subclass of DataDefinition.
  *
  * @param kVm the actual data definition represented as a Map
  * @param ec  the (implicit) execution context
  * @tparam K the key type
  * @tparam V the input value type
  */
case class EagerDD[K, V](kVm: Map[K, V])(implicit ec: ExecutionContext) extends BaseDD[K, V] with HasEvaluatedMap[K, V] {

  /**
    * Method to form a new DataDefinition where the resulting values derive from applying the function f to the original values
    *
    * @param f the function to transform key-value pairs
    * @tparam L the underlying type of the keys of the resulting map
    * @tparam W the underlying type of the values of the resulting map
    * @return a new DataDefinition
    */
  def map[L, W: Monoid](f: ((K, V)) => (L, W)): DataDefinition[L, W] = EagerDD(for ((k, v) <- kVm) yield f(k, v))

  /**
    * Method to filter this DataDefinition according to a predicate which takes a k-v tuple.
    *
    * @param p the predicate which will yield a Boolean for a given k-v tuple.
    * @return a new DataDefinition containing only those k-v pairs which satisfy the predicate p.
    */
  def filter(p: ((K, V)) => Boolean): DataDefinition[K, V] = EagerDD[K, V](kVm.filter(p))

  /**
    * Join method to perform inner join.
    *
    * @param other the DataDefinition with which to join this
    * @tparam L key type of other and also of the result
    * @tparam W value type of other
    * @return the inner join of this and other
    */
  def join[L >: K, W: Monoid](other: DataDefinition[L, W]): DataDefinition[L, (V, W)] = other match {
    case edd: EagerDD[L, W]@unchecked => EagerDD[L, (V, W)](joinMap(kVm.asInstanceOf[Map[L, V]], edd.kVm))
    case bdd: BaseDD[L, W]@unchecked =>
      import scala.concurrent.duration._
      implicit val timeout: Timeout = Timeout(5.seconds)
      join(Await.result(bdd.evaluate, timeout.duration))
    case _ => throw DataDefinitionException("join not supported for Eager and non-Eager DataDefinition objects")
  }

  /**
    * Return the evaluated map as is
    *
    * @return a Map[K,V]
    */
  def evalMap: Map[K, V] = kVm

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
    * @tparam L the new key type, whose values are derived from the values of this DataDefinition
    * @return a DataDefinition based on L and Iterable[V]
    */
  def groupBy[L](f: V => L): DataDefinition[L, Iterable[V]] = EagerDD(kVm.values.groupBy(f))
}

/**
  * Case Class which implements DataDefinition[K, W] and which is based on a Map[K,V] and a function V => W.
  *
  * @param kVm        the map of key-value pairs which serve as the input to this LazyDD
  * @param f          a function which will transform the key-value pairs
  * @param partitions the number of partitions to be used
  * @param context    a DDContext
  * @tparam K the key type
  * @tparam V the input value type
  * @tparam W the output value type
  */
case class LazyDD[K, V, L, W: Monoid](kVm: Map[K, V], f: ((K, V)) => (L, W))(partitions: Int = 2)(implicit context: DDContext) extends BaseDD[L, W]()(context.ec) {

  private implicit val cfs: Config = context.config
  private implicit val sys: ActorSystem = context.system
  private implicit val to: Timeout = context.timeout
  private implicit val ec: ExecutionContext = context.ec

  /**
    * Method to form a new DataDefinition where the resulting values derive from applying the function f to the original values
    *
    * @param g the function to transform key-value pairs
    * @tparam Y the underlying type of the keys of the resulting map
    * @tparam X the underlying type of the values of the resulting map
    * @return a new DataDefinition
    */
  def map[Y, X: Monoid](g: ((L, W)) => (Y, X)): DataDefinition[Y, X] = LazyDD[K, V, Y, X](kVm, f andThen g)(partitions)

  /**
    * Method to filter this DataDefinition according to a predicate which takes a l-w tuple.
    *
    * @param p the predicate which will yield a Boolean for a given l-w tuple.
    * @return a new DataDefinition containing only those l-w pairs which satisfy the predicate p.
    */
  def filter(p: ((L, W)) => Boolean): DataDefinition[L, W] = LazyDD[K, V, L, W](kVm.filter(f andThen p), f)(partitions)

  /**
    * Join method to perform inner join.
    *
    * @param other the DataDefinition with which to join this
    * @tparam M key type of other and also of the result
    * @tparam X value type of other
    * @return the inner join of this and other
    */
  def join[M >: L, X: Monoid](other: DataDefinition[M, X]): DataDefinition[M, (W, X)] = other match {
    case ldd: LazyDD[K, X, M, X]@unchecked =>
      import LazyDD._
      LazyDD[K, (V, X), M, (W, X)](joinMap(kVm, ldd.kVm), joinFunction(f, ldd.f))(partitions)
    case edd: EagerDD[M, X] => join(LazyDD[M, X, M, X](edd.kVm, identity)(partitions))
    case _ => throw DataDefinitionException("join not supported for Lazy and Base DataDefinition objects")
  }

  /**
    * Method to group values by a new key type generated from the values, ignoring the current keys.
    *
    * @tparam M the new key type, whose values are derived from the values of this DataDefinition
    * @return a DataDefinition based on M and Iterable[V]
    */
  def groupBy[M](f: W => M): DataDefinition[M, Iterable[W]] = {
    implicit object IterableMonoidW extends IterableMonoid[W]
    DataDefinition(applyFunction.values.groupBy(f))
  }

  /**
    * Evaluate this LazyDD as a Future of DataDefinition[L,W] with HasEvaluatedMap[L,W]
    *
    * @return an EagerDD[L,W] wrapped in Future
    */
  def evaluate: Future[DataDefinition[L, W] with HasEvaluatedMap[L, W]] =
    if (partitions < 2) Future(EagerDD(applyFunction))(scala.concurrent.ExecutionContext.Implicits.global)
    else {
      implicit val actors: Actors = Actors(implicitly[ActorSystem], implicitly[Config])
      val mr = MapReducePipe.create[K, V, L, W, W]((k, v) => f((k, v)), implicitly[Monoid[W]].combine, 1)
      context.register(mr)
      for (x: Map[L, W] <- mr(kVm.toSeq)) yield EagerDD(x)
    }

  private def applyFunction: Map[L, W] = for ((k, v) <- kVm) yield f(k, v)

  /**
    * Clean up any resources in the context of this LazyDD object
    */
  def clean(): Unit = context.clean()

}

/**
  * This trait is essentially a private trait: only to be used by this module.
  *
  * @tparam K the key type
  * @tparam V the value type
  */
sealed trait HasEvaluatedMap[K, V] {
  def evalMap: Map[K, V]
}

/**
  * Abstract base class which implements the generic DataDefinition[K, V].
  *
  * @param ec an ExecutionContext
  * @tparam K the key type
  * @tparam V the input value type
  */
abstract class BaseDD[K, V](implicit ec: ExecutionContext) extends DataDefinition[K, V] {

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
    * @param xv_x the aggregation function.
    * @tparam X the underlying type of the result.
    * @return an X value, wrapped in Future.
    */
  def reduce[X: Zero](xv_x: (X, V) => X): Future[X] = for (kVm <- apply()) yield kVm.values.foldLeft(implicitly[Zero[X]].zero)(xv_x)

  /**
    * Evaluate the number of elements in this DataDefinition
    *
    * @return the number of k-v pairs
    */
  def count: Future[Int] = for (kVm <- apply()) yield kVm.size
}

/**
  * The context in which DataDefinition instances will be evaluated
  *
  * @param config  the configuration
  * @param system  the actor system
  * @param timeout the value of timeout
  */
case class DDContext(config: Config, system: ActorSystem, timeout: Timeout)(implicit executor: ExecutionContext) {
  // NOTE: consciously using var here.
  var closeables: List[AutoCloseable] = Nil

  def clean(): Unit = {
    for (closeable <- closeables) closeable.close()
    closeables = Nil
  }

  def register(cs: AutoCloseable*): Unit = {
    closeables = closeables ++ cs
  }

  def ec: ExecutionContext = executor

  override def toString: String = s"DDContext: system=${system.name}, timeout=$timeout"
}

object DDContext {

  import java.util.concurrent.TimeUnit

  def apply(implicit executor: ExecutionContext): DDContext = {
    val config = ConfigFactory.load().getConfig("DataDefinition")
    val timeout = FiniteDuration(config.getDuration("timeout").getSeconds, TimeUnit.SECONDS)
    val system: ActorSystem = ActorSystem(config.getString("actorSystem"))
    apply(config, system, timeout)
  }
}

object DataDefinition {

  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val context: DDContext = DDContext.apply

  def apply[K, V: Monoid](kVs: Map[K, V], partitions: Int): DataDefinition[K, V] = LazyDD(kVs, identity[(K, V)])(partitions)

  def apply[K, V: Monoid](kVs: Map[K, V]): DataDefinition[K, V] = LazyDD(kVs, identity[(K, V)])()

  def apply[K, V: Monoid](vs: Seq[V], f: V => K, partitions: Int): DataDefinition[K, V] = apply((for (v <- vs) yield (f(v), v)).toMap, partitions)

  def apply[K, V: Monoid](vs: Seq[V], f: V => K): DataDefinition[K, V] = apply(vs, f, 2)

  def apply[K, V: Monoid](vs: Seq[(K, V)], partitions: Int): DataDefinition[K, V] = apply(vs.toMap, partitions)

  def apply[K, V: Monoid](vs: Seq[(K, V)]): DataDefinition[K, V] = apply(vs.toMap)

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

  trait IterableMonoid[T] extends Monoid[Iterable[T]] {
    def zero: Iterable[T] = Seq[T]()

    def combine(x: Iterable[T], y: Iterable[T]): Iterable[T] = x ++ y
  }

  private def vToWToTupleToTuple[K, V, W](f: V => W)(t: (K, V)): (K, W) = (t._1, f(t._2))
}

object LazyDD {
  private[majabigwaduce] def joinMap[K, V, W](map1: Map[K, V], map2: Map[K, W]): Map[K, (V, W)] = (for (key <- map1.keySet intersect map2.keySet) yield (key, (map1(key), map2(key)))).toMap

  private def joinFunction[K, V, L, W, X, Y](f: ((K, V)) => (L, W), g: ((K, X)) => (L, Y)): ((K, (V, X))) => (L, (W, Y)) = {
    case (k, (v, x)) =>
      val vKf = f(k, v)
      vKf._1 -> (vKf._2, g(k, x)._2)
  }
}

case class DataDefinitionException(str: String) extends Exception(str)

//case object IterableMonoid[T] extends Iterable[T] with Monoid[Iterable[T]] {
//
//  val mt = implicitly[Monoid[T]]
//
//  def iterator: Iterator[T] = iterable.toIterator
//
//  def zero: Iterable[T] = Seq[T]()
//
//  def combine(x: Iterable[T], y: Iterable[T]): Iterable[T] = x ++ y
//}