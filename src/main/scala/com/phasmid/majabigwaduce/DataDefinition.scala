package com.phasmid.majabigwaduce


import akka.actor.ActorSystem
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

/**
  * Trait to represent a "data definition" (similar to RDD in Spark)
  *
  * Created by scalaprof on 10/31/16.
  *
  * @tparam K the key type
  * @tparam V the value type
  */
trait DataDefinition[K, V] extends (() => Future[Map[K, V]]) {

  def map[W: Monoid](f: V => W): DataDefinition[K, W]

  def apply(): Future[Map[K, V]]

  def aggregate[W: Zero](f: (W, V) => W): Future[W]

  def clean(): Unit
}

case class LazyDD[K, V, W: Monoid](map: Map[K, V], f: (V) => W)(partitions: Int = 2)(implicit context: DDContext) extends DataDefinition[K, W] {

  import scala.concurrent.ExecutionContext.Implicits.global

  private implicit val cfs: Config = context.config
  private implicit val sys: ActorSystem = context.system
  private implicit val to: Timeout = context.timeout

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

case class DDContext(config: Config, system: ActorSystem, timeout: Timeout) {
  var closeables: List[AutoCloseable] = Nil

  def clean(): Unit = {
    for (closeable <- closeables) closeable.close()
    closeables = Nil
  }

  def register(cs: AutoCloseable*): Unit = {
    closeables = closeables ++ cs
  }

  override def toString: String = s"DDContext: system=${system.name}, timeout=$timeout"
}

object DDContext {

  import java.util.concurrent.TimeUnit

  def apply: DDContext = {
    val config = ConfigFactory.load()
    val timeout = FiniteDuration(config.getDuration("timeout").getSeconds, TimeUnit.SECONDS)
    val system: ActorSystem = ActorSystem(config.getString("actorSystem"))
    apply(config, system, timeout)
  }
}

object DataDefinition {

  implicit val context: DDContext = DDContext.apply

  def apply[K, V: Monoid](k_vs: Map[K, V], partitions: Int): DataDefinition[K, V] = LazyDD(k_vs, identity[V])(partitions)

  def apply[K, V: Monoid](k_vs: Map[K, V]): DataDefinition[K, V] = LazyDD(k_vs, identity[V])()

  def apply[K, V: Monoid](vs: Seq[V], f: V => K, partitions: Int): DataDefinition[K, V] = apply((for (v <- vs) yield (f(v), v)).toMap, partitions)

  def apply[K, V: Monoid](vs: Seq[V], f: V => K): DataDefinition[K, V] = apply(vs, f, 2)
}