/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce

import akka.actor.Props
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent._
import scala.util.Try






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
case class MapReducePipe[K0, V0, K1, W, V1 >: W](f: (K0, V0) => Try[(K1, W)], g: (V1, W) => V1, n: Int)(implicit actors: Actors, timeout: Timeout) extends MapReduce_LoggingBase[(K0, V0), K1, V1](actors)(timeout) {
  def createProps: Props = Props(new Master(actors.config, f, g))

  override def createName: Option[String] = Some(s"""${MapReducePipe.sMrpMaster}-$n""")
}

object MapReducePipe {
  // The following apply method allows for a f which needs to be lifted to T=>Try[R]
  def create[K0, V0, K1, W, V1 >: W](f: (K0, V0) => (K1, W), g: (V1, W) => V1, n: Int)(implicit actors: Actors, timeout: Timeout): MapReducePipe[K0, V0, K1, W, V1] =
    apply(MapReduce.lift(f), g, n)(actors, timeout)

  //noinspection SpellCheckingInspection
  val sMrpMaster: String = "mrf-mstr"
}

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
case class MapReduceFirstFold[V0, K1, W, V1: Zero](f: V0 => Try[(K1, W)], g: (V1, W) => V1)(actors: Actors, timeout: Timeout) extends MapReduce_LoggingBase[V0, K1, V1](actors)(timeout) {
  // The following constructor allows for a f which needs to be lifted to T=>Try[R]
  // CONSIDER implementing an apply method in MapReduce for this signature
  //  def this(fy: V0 => (K1, W), g: (V1, W) => V1)(actors: Actors, timeout: Timeout) = this(MapReduce.lift(fy), g)(actors, timeout)
  def createProps: Props = Props(new Master_First_Fold(actors.config, f, g, implicitly[Zero[V1]].zero _))

  override def createName: Option[String] = Some(MapReduceFirstFold.sMrffMstr)
}

object MapReduceFirstFold {
  def create[V0, K1, W, V1: Zero](f: V0 => (K1, W), g: (V1, W) => V1)(actors: Actors, timeout: Timeout): MapReduceFirstFold[V0, K1, W, V1] =
    apply(MapReduce.lift(f), g)(actors, timeout)

  //noinspection SpellCheckingInspection
  val sMrffMstr = "mrff-mstr"
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
  * @param actors  an instance of Actors.
  * @param timeout the value of timeout to be used
  */
case class MapReducePipeFold[K0, V0, K1, W, V1: Zero](f: (K0, V0) => Try[(K1, W)], g: (V1, W) => V1, n: Int)(actors: Actors, timeout: Timeout) extends MapReduce_LoggingBase[(K0, V0), K1, V1](actors)(timeout) {
  // The following constructor allows for a f which needs to be lifted to T=>Try[R]
  //  def this(fy: (K0, V0) => (K1, W), g: (V1, W) => V1, n: Int)(actors: Actors, timeout: Timeout) = this(MapReduce.lift(fy), g, n)(actors, timeout)
  def createProps: Props = Props(new Master_Fold(actors.config, f, g, implicitly[Zero[V1]].zero _))

  override def createName: Option[String] = Some(s"""${MapReducePipeFold.sMRPFMaster}-$n""")
}

object MapReducePipeFold {
  def create[K0, V0, K1, W, V1: Zero](f: (K0, V0) => (K1, W), g: (V1, W) => V1, n: Int)(actors: Actors, timeout: Timeout): MapReducePipeFold[K0, V0, K1, W, V1] =
    apply(MapReduce.lift(f), g, n)(actors, timeout)

  //noinspection SpellCheckingInspection
  val sMRPFMaster = "mrpf-mstr"
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
  * @param actors  an instance of Actors
  * @param timeout the value of timeout to be used
  */
abstract class MapReduce_LoggingBase[T, K1, V1](actors: Actors)(timeout: Timeout) extends MapReduce_Base[T, K1, V1](actors)(timeout) {
  def logException(m: => String, x: Throwable): Unit = actors.logException(m, x)
}

/**
  * An abstract base class for MapReduce classes (other than MapReduceComposed).
  */
abstract class MapReduce_Base[T, K, V](actors: Actors)(implicit timeout: Timeout) extends MapReduce[T, K, V] {
  self =>
  implicit def ec: ExecutionContextExecutor = actors.system.dispatcher

  private val master = actors.createActor(actors.system, createName, createProps)

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
  def createName: Option[String] = None

  def report(vKr: Response[K, V]): Boolean = {
    for ((k, x) <- vKr.left) logException(s"exception thrown (but forgiven) for key $k", x)
    vKr.size == 0
  }

  def logException(m: => String, x: Throwable): Unit

  def close(): Unit = actors.system.stop(master)
}


