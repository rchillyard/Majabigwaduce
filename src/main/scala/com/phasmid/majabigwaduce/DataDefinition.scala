/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce


import akka.actor.ActorSystem
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

/**
  * Trait to represent a "data definition" (similar to RDD in Spark).
  * Essentially, a DataDefinition[K, V] is a function which transforms Unit into a Future[Map[K,V].
  * Like RDD, it is lazy and can be partitioned.
  * In order to yield a concrete value, there are two methods which may be called: apply() and aggregate(f).
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
    * @param f the function to transform values
    * @tparam W the underlying type of the values of the resulting map
    * @return a new DataDefinition
    */
  def map[W: Monoid](f: V => W): DataDefinition[K, W]

  /**
    * Evaluate this DataDefinition
    *
    * @return a map of key-value pairs wrapped in Future
    */
  def apply(): Future[Map[K, V]]

  /**
    * Evaluate this DataDefintion but reduce the dimensionality of the result by ignoring the keys
    * and aggregating the values according to the function f
    *
    * @param f the aggregation function
    * @tparam W the underlying type of the result
    * @return a W value, wrapped in Future.
    */
  def aggregate[W: Zero](f: (W, V) => W): Future[W]

  /**
    * Clean up any residual resources from this DataDefinition
    */
  def clean(): Unit
}

/**
  * Case Class which implements DataDefinition[K, W] and which is based on a Map[K,V] and a function V => W.
  *
  * CONSIDER implementing a filter also
  *
  * @param map        the map of key-value pairs which serve as the input to this LazyDD
  * @param f          a function which will transform the values the the KV pairs
  * @param partitions the number of partitions to be used
  * @param context    a DDContext
  * @tparam K the key type
  * @tparam V the input value type
  * @tparam W the output value type
  */
case class LazyDD[K, V, W: Monoid](map: Map[K, V], f: (V) => W)(partitions: Int = 2)(implicit context: DDContext) extends DataDefinition[K, W] {

  private implicit val cfs: Config = context.config
  private implicit val sys: ActorSystem = context.system
  private implicit val to: Timeout = context.timeout
  private implicit val ec: ExecutionContext = context.ec

  def map[X: Monoid](g: W => X): LazyDD[K, V, X] = LazyDD(map, f andThen g)(partitions)

  def apply(): Future[Map[K, W]] =
    if (partitions < 2) Future {
      for ((k, v) <- map; w = f(v)) yield (k, w)
    }
    else {
      val mr = MapReducePipe[K, V, K, W, W]((k, v) => (k, f(v)), implicitly[Monoid[W]].combine, 1)
      context.register(mr)
      mr(map.toSeq)
    }

  def clean(): Unit = context.clean()

  /**
    * Method to apply an aggregate (i.e. reduce) function to the result of invoking apply
    *
    * @param g the aggregation function
    * @tparam X the return type
    * @return the returned value wrapped in Future
    */
  def aggregate[X: Zero](g: (X, W) => X): Future[X] = for (kWm <- apply()) yield kWm.values.foldLeft(implicitly[Zero[X]].zero)(g)
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

  def apply[K, V: Monoid](k_vs: Map[K, V], partitions: Int): DataDefinition[K, V] = LazyDD(k_vs, identity[V])(partitions)

  def apply[K, V: Monoid](k_vs: Map[K, V]): DataDefinition[K, V] = LazyDD(k_vs, identity[V])()

  def apply[K, V: Monoid](vs: Seq[V], f: V => K, partitions: Int): DataDefinition[K, V] = apply((for (v <- vs) yield (f(v), v)).toMap, partitions)

  def apply[K, V: Monoid](vs: Seq[V], f: V => K): DataDefinition[K, V] = apply(vs, f, 2)

  def apply[K, V: Monoid](vs: Seq[(K, V)], partitions: Int): DataDefinition[K, V] = apply(vs.toMap, partitions)

  def apply[K, V: Monoid](vs: Seq[(K, V)]): DataDefinition[K, V] = apply(vs.toMap)
}