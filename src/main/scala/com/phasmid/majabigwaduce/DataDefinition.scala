/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce

import akka.actor.ActorSystem
import akka.util.Timeout
import com.phasmid.majabigwaduce.LazyDD.joinMap
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

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
trait DataDefinition[K, V] extends (() => Future[Map[K, V]]) {

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
    * Method to evaluate this DataDefintion and reduce the dimensionality of the result by ignoring the keys
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

case class EagerDD[K, V](kVm: Map[K, V])(implicit context: DDContext) extends BaseDD[K, V] {

  private implicit val ec: ExecutionContext = context.ec

  /**
    * Evaluate this DataDefinition
    *
    * @return a map of key-value pairs wrapped in Future
    */
  override def apply(): Future[Map[K, V]] = Future(kVm)

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
    case _ => throw DataDefinitionException("join not supported for Eager and non-Eager DataDefinition objects")
  }
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
case class LazyDD[K, V, L, W: Monoid](kVm: Map[K, V], f: ((K, V)) => (L, W))(partitions: Int = 2)(implicit context: DDContext) extends BaseDD[L, W] {

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
    * Evaluate this LazyDD as a Future[Map[L, W]
    *
    * @return a map of key-value pairs wrapped in Future
    */
  def apply(): Future[Map[L, W]] =
    if (partitions < 2) Future {
      for ((k, v) <- kVm) yield f(k, v)
    }
    else {
      val mr = MapReducePipe[K, V, L, W, W]((k, v) => f((k, v)), implicitly[Monoid[W]].combine, 1)
      context.register(mr)
      mr(kVm.toSeq)
    }

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
  }
}

/**
  * Abstract base class which implements the generic DataDefinition[K, V].
  *
  * @param context a DDContext
  * @tparam K the key type
  * @tparam V the input value type
  */
abstract class BaseDD[K, V](implicit context: DDContext) extends DataDefinition[K, V] {

  private implicit val ec: ExecutionContext = context.ec

  /**
    * Clean up any resources in the context of this LazyDD object
    */
  def clean(): Unit = context.clean()

  /**
    * Method to evaluate this DataDefintion and reduce the dimensionality of the result by ignoring the keys
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
    val config = ConfigFactory.load()
    val timeout = FiniteDuration(config.getDuration("timeout").getSeconds, TimeUnit.SECONDS)
    val system: ActorSystem = ActorSystem(config.getString("actorSystem"))
    apply(config, system, timeout)
  }
}

object DataDefinition {

  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val context: DDContext = DDContext.apply

  def apply[K, V: Monoid](k_vs: Map[K, V], partitions: Int): DataDefinition[K, V] = LazyDD(k_vs, identity[(K, V)])(partitions)

  def apply[K, V: Monoid](k_vs: Map[K, V]): DataDefinition[K, V] = LazyDD(k_vs, identity[(K, V)])()

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
  def tupleLift[K, V, W](f: V => W): (((K, V)) => (K, W)) = vToWToTupleToTuple(f)

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