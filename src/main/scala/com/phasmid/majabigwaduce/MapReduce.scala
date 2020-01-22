/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce

import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.Config

import scala.concurrent._

/**
  * MapReduce is a trait, with case classes, which implements a functional API for the map-reduce classes in this package.
  *
  * @author scalaprof
  * @tparam T  the input type of the MapReduce function: T may be V1 for a first stage, or (K1,V1) for a subsequent stage.
  * @tparam K1 the key type of the returned map
  * @tparam V1 the value type of the returned map
  */
trait MapReduce[T, K1, V1] extends ASync[Seq[T], Map[K1, V1]] with AutoCloseable {

  self =>

  /**
    * Compose this MapReduce object with mr, yielding a new MapReduce object.
    *
    * @tparam K2 the key type of the composed MapReduce object
    * @tparam V2 the value type of the composed MapReduce object
    * @param f a function of type ASync[Seq[(K1, V1)], Map[K2, V2], i.e. Seq[(K1, V1)]=>Future[Map[K2, V2]
    * @return a new MapReduceComposed object
    */
  def :&[K2, V2](f: ASync[Seq[(K1, V1)], Map[K2, V2]]): MapReduce[T, K2, V2] = MapReduceComposed(self, f)(self.ec)

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
  def :|[S](r: RF[K1, V1, S])(implicit executionContext: ExecutionContext): ASync[Seq[T], S] = ts => for (v2K2m <- self(ts); s = r(v2K2m)) yield s

  /**
    * alternative name to terminate
    *
    * @param r                the Reduce object
    * @param executionContext (implicit)
    * @tparam S the return type, which is a super-class of V1 (for sum, or sigma)
    * @return an Async function of Seq[T]=>Future[S] type S.
    */
  def |[S](r: RF[K1, V1, S])(implicit executionContext: ExecutionContext): ASync[Seq[T], S] = :|(r)(executionContext)

  /**
    * @return a suitable execution context
    */
  def ec: ExecutionContext
}

/**
  * A first-stage MapReduce class where the result type V1 is a super-type of the intermediate type W
  *
  * @tparam V0 input value type
  * @tparam K1 output key type
  * @tparam W  intermediate type
  * @tparam V1 output value type (super-type of W)
  * @param f       the mapper function which takes a V0 instance and creates a key-value tuple of type (K1,W)
  * @param g       the reducer function which combines two values (an V1 and a W) into one V1
  * @param config  an instance of Config which defines a suitable configuration
  * @param system  the actor system
  * @param timeout the value of timeout to be used
  */
case class MapReduceFirst[V0, K1, W, V1 >: W](f: V0 => (K1, W), g: (V1, W) => V1)(implicit config: Config, system: ActorSystem, timeout: Timeout) extends MapReduce_LoggingBase[V0, K1, V1](config, system)(timeout) {
  def createProps: Props = Props(new Master_First(config, f, g))

  //  def createName(): Option[String] = s"""mrf-mstr"""
}

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
  * @param config  an instance of Config which defines a suitable configuration
  * @param system  the actor system
  * @param timeout the value of timeout to be used
  */
case class MapReducePipe[K0, V0, K1, W, V1 >: W](f: (K0, V0) => (K1, W), g: (V1, W) => V1, n: Int)(implicit config: Config, system: ActorSystem, timeout: Timeout) extends MapReduce_LoggingBase[(K0, V0), K1, V1](config, system)(timeout) {
  def createProps: Props = Props(new Master(config, f, g))

  //  def createName(): Option[String] = s"""mrp-mstr-$n"""
}

/**
  * A first-stage MapReduce class
  *
  * @tparam V0 input value type
  * @tparam K1 output key type
  * @tparam W  intermediate type
  * @tparam V1 output value type (must support type class Init)
  * @param f       the mapper function which takes a V0 instance and creates a key-value tuple of type (K1,W)
  * @param g       the reducer function which combines two values (an V1 and a W) into one V1
  * @param config  an instance of Config which defines a suitable configuration
  * @param system  the actor system
  * @param timeout the value of timeout to be used
  */
case class MapReduceFirstFold[V0, K1, W, V1: Zero](f: V0 => (K1, W), g: (V1, W) => V1)(config: Config, system: ActorSystem, timeout: Timeout) extends MapReduce_LoggingBase[V0, K1, V1](config, system)(timeout) {
  def createProps: Props = Props(new Master_First_Fold(config, f, g, implicitly[Zero[V1]].zero _))

  //  def createName(): Option[String] =  s"""mrff-mstr"""
}

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
  * @param config  an instance of Config which defines a suitable configuration
  * @param system  the actor system
  * @param timeout the value of timeout to be used
  */
case class MapReducePipeFold[K0, V0, K1, W, V1: Zero](f: (K0, V0) => (K1, W), g: (V1, W) => V1, n: Int)(config: Config, system: ActorSystem, timeout: Timeout) extends MapReduce_LoggingBase[(K0, V0), K1, V1](config, system)(timeout) {
  def createProps: Props = Props(new Master_Fold(config, f, g, implicitly[Zero[V1]].zero _))

  //  def createName(): Option[String] = s"""mrpf-mstr-$n"""
}

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
case class MapReduceComposed[T, K1, V1, K2, V2](f1: MapReduce[T, K1, V1], f2: ASync[Seq[(K1, V1)], Map[K2, V2]])(implicit val ec: ExecutionContext) extends MapReduce[T, K2, V2] {
  def apply(ts: Seq[T]): Future[Map[K2, V2]] = for (v2K2m: Map[K1, V1] <- f1(ts); v3K3m: Map[K2, V2] <- f2(v2K2m.toSeq)) yield v3K3m

  def close(): Unit = f1.close()
}

/**
  * A reduce function which can be composed (on the right) with a MapReduce object.
  *
  * @param f the function which will combine the current result with each element of an input set
  * @tparam T the input (free) type of this reduction
  * @tparam S the output (derived) type of this reduction
  */
case class Reduce[K, T, S: Zero](f: (S, T) => S) extends RF[K, T, S] {
  /**
    * This method cannot use reduce because, logically, reduce is not able to process an empty collection.
    * Note that we ignore the keys of the input map (m)
    *
    * @param m the input map (keys will be ignored)
    * @return the result of combining all values of m, using the f function.
    *         An empty map will result in the value of z() being returned.
    */
  def apply(m: Map[K, T]): S = m.values.foldLeft(implicitly[Zero[S]].zero)(f)
}

/**
  * An abstract base class which extends MapReduce_Base and which implements the logException method with non-trivial logging.
  *
  * @tparam T  the input type of the MapReduce function: T may be V1 for a first stage, or (K1,V1) for a subsequent stage.
  * @tparam K1 intermediate key type
  * @tparam V1 intermediate value type
  * @param config  an instance of Config which defines a suitable configuration
  * @param system  the actor system
  * @param timeout the value of timeout to be used
  */
abstract class MapReduce_LoggingBase[T, K1, V1](config: Config, system: ActorSystem)(implicit timeout: Timeout) extends MapReduce_Base[T, K1, V1](system)(timeout) {
  private val exceptionStack = config.getBoolean("exceptionStack")

  def logException(m: String, x: Throwable): Unit = if (exceptionStack) system.log.error(x, m) else system.log.warning(s"$m: ${x.getLocalizedMessage}")
}

/**
  * An abstract base class for MapReduce classes (other than MapReduceComposed).
  */
abstract class MapReduce_Base[T, K, V](system: ActorSystem)(implicit timeout: Timeout) extends MapReduce[T, K, V] {
  self =>
  implicit def ec: ExecutionContextExecutor = system.dispatcher

  private val master = createName() match {
    case Some(name) => system.actorOf(createProps, name)
    case None => system.actorOf(createProps)
  }

  def apply(ts: Seq[T]): Future[Map[K, V]] = {
    // Note: currently, we ignore the value of report but we could pass back a tuple that includes ok and the resulting map
    for (vKr <- master.ask(ts).mapTo[Response[K, V]]; _ = report(vKr)) yield vKr.right
  }

  def createProps: Props

  /**
    * This probably ought to be configured according to whether or not we are debugging
    *
    * @return
    */
  def createName(): Option[String] = None

  def report(vKr: Response[K, V]): Boolean = {
    for ((k, x) <- vKr.left) logException(s"exception thrown (but forgiven) for key $k", x)
    vKr.size == 0
  }

  def logException(m: String, x: Throwable): Unit

  def close(): Unit = system.stop(master)
}

