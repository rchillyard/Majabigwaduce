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
    * @param f the function to transform key-value pairs
    * @tparam L the underlying type of the keys of the resulting map
    * @tparam W the underlying type of the values of the resulting map
    * @return a new DataDefinition
    */
  def map[L, W: Monoid](f: ((K,V)) => (L,W)): DataDefinition[L, W]

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

  def count: Future[Int]

  def filter(f: ((K,V)) => Boolean): DataDefinition[K,V]

  def join[W: Monoid](o:DataDefinition[K,W]): DataDefinition[K,(V,W)]
}

/**
  * Case Class which implements DataDefinition[K, W] and which is based on a Map[K,V] and a function V => W.
  *
  * CONSIDER implementing a filter also
  *
  * @param kVm        the map of key-value pairs which serve as the input to this LazyDD
  * @param f          a function which will transform the key-value pairs
  * @param partitions the number of partitions to be used
  * @param context    a DDContext
  * @tparam K the key type
  * @tparam V the input value type
  * @tparam W the output value type
  */
case class LazyDD[K, V, L, W: Monoid](kVm: Map[K, V], f: ((K,V)) => (L,W))(partitions: Int = 2)(implicit context: DDContext) extends DataDefinition[L, W] {

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
  def map[Y, X: Monoid](g: ((L, W)) => (Y, X)): DataDefinition[Y, X] = LazyDD[K,V,Y,X](kVm, f andThen g)(partitions)

  /**
    * Evaluate this LazyDD as a Future[Map[L, W]
    *
    * @return a map of key-value pairs wrapped in Future
    */
  def apply(): Future[Map[L, W]] =
    if (partitions < 2) Future {
      for ((k, v) <- kVm) yield f(k,v)
    }
    else {
      val mr = MapReducePipe[K, V, L, W, W]((k,v) => f((k,v)), implicitly[Monoid[W]].combine, 1)
      context.register(mr)
      mr(kVm.toSeq)
    }

  /**
    * Clean up any resources in the context of this LazyDD object
    */
  def clean(): Unit = context.clean()

  /**
    * Method to apply an aggregate (i.e. reduce) function to the result of invoking apply
    *
    * @param g the aggregation function
    * @tparam X the return type
    * @return the returned value wrapped in Future
    */
  def aggregate[X: Zero](g: (X, W) => X): Future[X] = for (kWm <- apply()) yield kWm.values.foldLeft(implicitly[Zero[X]].zero)(g)

  def count: Future[Int] = for (kWm <- apply()) yield kWm.size

  def filter(g: ((L, W)) => Boolean):DataDefinition[L, W] = LazyDD[K,V,L,W](kVm.filter(f andThen g), f)(partitions)

  /**
    * Join method to perform inner join.
    *
    * CONSIDER: (YY) Not sure why f change the type of key, or why ((K,V)) => (L,W) not ((K,V)) => (K,W), also when join two function,
    * the value of [K,(V,X),L,(W,X)]'s L come from left side LazyDD, there will be a problem when left side and right side have different
    * function to transform K to L.
    *
    * TODO: (RCH) remove the asInstanceOf calls if possible
    *
    * @param o
    * @tparam X
    * @return
    */
  def join[X: Monoid](o: DataDefinition[L, X]):DataDefinition[L,(W,X)] = LazyDD[K,(V,X),L,(W,X)](joinMap(kVm,o.asInstanceOf[LazyDD[K,X,L,X]].kVm), joinFunction(f,o.asInstanceOf[LazyDD[K,X,L,X]].f))(partitions)

  private def joinMap[KK,VV,WW](map1: Map[KK, VV], map2: Map[KK, WW]) = (for(key <- (map1.keySet intersect map2.keySet).toIterator)
    yield (key, (map1(key), map2(key)))).toMap

  private def joinFunction[KK,VV,LL,WW,XX,YY](f:((KK,VV)) => (LL,WW),g:((KK,XX)) => (LL,YY)):((KK,(VV,XX)))=>(LL,(WW,YY)) =
    i => (f(i._1,i._2._1)._1,(f(i._1,i._2._1)._2,g(i._1,i._2._2)._2))
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

  def apply[K, V: Monoid](k_vs: Map[K, V], partitions: Int): DataDefinition[K, V] = LazyDD(k_vs, identity[(K,V)])(partitions)

  def apply[K, V: Monoid](k_vs: Map[K, V]): DataDefinition[K, V] = LazyDD(k_vs, identity[(K,V)])()

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
  def tupleLift[K, V, W](f: V=>W): (((K,V))=>(K, W)) = vToWToTupleToTuple(f)

  private def vToWToTupleToTuple[K, V, W](f: V=>W)(t: (K,V)): (K,W) = (t._1, f(t._2))
}
