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
  *
  *         CONSIDER renumbering the K, V types so that the input to the first stage is Seq[V0] and the output of the nth stage is Map[Kn,Vn]
  * @tparam T  the input type of the MapReduce function: T may be V1 for a first stage, or (K1,V1) for a subsequent stage.
  * @tparam K2 the key type of the returned map
  * @tparam V2 the value type of the returned map
  */
trait MapReduce[T, K2, V2] extends ASync[Seq[T], Map[K2, V2]] {

  self =>

  /**
    * compose this MapReduce object with mr, yielding a new MapReduce object.
    *
    * @tparam K3 the key type of the composed MapReduce object
    * @tparam V3 the value type of the composed MapReduce object
    * @param mr the other MapReduce object
    * @return a new MapReduceComposed object
    */
  def compose[K3, V3](mr: ASync[Seq[(K2, V2)], Map[K3, V3]]): MapReduce[T, K3, V3] = MapReduceComposed(self, mr)(self.ec)

  /**
    * alternative name for compose
    *
    * @param mr the other MapReduce object
    * @tparam K3 the key type of the composed MapReduce object
    * @tparam V3 the value type of the composed MapReduce object
    * @return a new MapReduceComposed object
    */
  def &[K3, V3](mr: ASync[Seq[(K2, V2)], Map[K3, V3]]): MapReduce[T, K3, V3] = compose(mr)

  /**
    * terminate this MapReduce object with r, a reducer which yields a simple value
    *
    * @param r                the Reduce object
    * @param executionContext (implicit)
    * @tparam S the return type, which is a super-class of V2 (for sum, or sigma)
    * @return an Async function of Seq[T]=>Future[S] type S.
    */
  def terminate[S](r: RF[K2, V2, S])(implicit executionContext: ExecutionContext): ASync[Seq[T], S] = ts => for (v2K2m <- self(ts); s = r(v2K2m)) yield s

  /**
    * alternative name to terminate
    *
    * @param r                the Reduce object
    * @param executionContext (implicit)
    * @tparam S the return type, which is a super-class of V2 (for sum, or sigma)
    * @return an Async function of Seq[T]=>Future[S] type S.
    */
  def |[S](r: RF[K2, V2, S])(implicit executionContext: ExecutionContext): ASync[Seq[T], S] = terminate(r)(executionContext)

  /**
    * @return a suitable execution context
    */
  def ec: ExecutionContext
}

/**
  * A first-stage MapReduce class where the result type V2 is a super-type of the intermediate type W
  *
  * @tparam V1 input value type
  * @tparam K2 output key type
  * @tparam W  intermediate type
  * @tparam V2 output value type (super-type of W)
  * @param f       the mapper function which takes a V1 instance and creates a key-value tuple of type (K2,W)
  * @param g       the reducer function which combines two values (an V2 and a W) into one V2
  * @param config  an instance of Config which defines a suitable configuration
  * @param system  the actor system
  * @param timeout the value of timeout to be used
  */
case class MapReduceFirst[V1, K2, W, V2 >: W](f: V1 => (K2, W), g: (V2, W) => V2)(implicit config: Config, system: ActorSystem, timeout: Timeout) extends MapReduce_LoggingBase[V1, K2, V2](config, system)(timeout) {
  def createProps = Props(new Master_First(config, f, g))

  def createName = s"""mrf-mstr"""
}

/**
  * A later-stage MapReduce class where the result type V2 is a super-type of the intermediate type W
  *
  * @tparam K1 input key type
  * @tparam V1 input value type
  * @tparam K2 output key type
  * @tparam W  intermediate type
  * @tparam V2 output value type (super-type of W)
  * @param f       the mapper function which takes a K1,V1 pair and creates a key-value tuple of type (K2,W)
  * @param g       the reducer function which combines two values (an V2 and a W) into one V2
  * @param n       the stage number of this map-reduce stage.
  * @param config  an instance of Config which defines a suitable configuration
  * @param system  the actor system
  * @param timeout the value of timeout to be used
  */
case class MapReducePipe[K1, V1, K2, W, V2 >: W](f: (K1, V1) => (K2, W), g: (V2, W) => V2, n: Int)(implicit config: Config, system: ActorSystem, timeout: Timeout) extends MapReduce_LoggingBase[(K1, V1), K2, V2](config, system)(timeout) {
  def createProps = Props(new Master(config, f, g))

  def createName = s"""mrp-mstr-$n"""
}

/**
  * A first-stage MapReduce class
  *
  * @tparam V1 input value type
  * @tparam K2 output key type
  * @tparam W  intermediate type
  * @tparam V2 output value type (must support type class Init)
  * @param f       the mapper function which takes a V1 instance and creates a key-value tuple of type (K2,W)
  * @param g       the reducer function which combines two values (an V2 and a W) into one V2
  * @param config  an instance of Config which defines a suitable configuration
  * @param system  the actor system
  * @param timeout the value of timeout to be used
  */
case class MapReduceFirstFold[V1, K2, W, V2: Zero](f: V1 => (K2, W), g: (V2, W) => V2)(config: Config, system: ActorSystem, timeout: Timeout) extends MapReduce_LoggingBase[V1, K2, V2](config, system)(timeout) {
  def createProps = Props(new Master_First_Fold(config, f, g, implicitly[Zero[V2]].zero _))

  def createName = s"""mrff-mstr"""
}

/**
  * A later-stage MapReduce class
  *
  * @tparam K1 input key type
  * @tparam V1 input value type
  * @tparam K2 output key type
  * @tparam W  intermediate type
  * @tparam V2 output value type (must support type class Init)
  * @param f       the mapper function which takes a V1 instance and creates a key-value tuple of type (K2,W)
  * @param g       the reducer function which combines two values (an V2 and a W) into one V2
  * @param n       the stage number of this map-reduce stage.
  * @param config  an instance of Config which defines a suitable configuration
  * @param system  the actor system
  * @param timeout the value of timeout to be used
  */
case class MapReducePipeFold[K1, V1, K2, W, V2: Zero](f: (K1, V1) => (K2, W), g: (V2, W) => V2, n: Int)(config: Config, system: ActorSystem, timeout: Timeout) extends MapReduce_LoggingBase[(K1, V1), K2, V2](config, system)(timeout) {
  def createProps = Props(new Master_Fold(config, f, g, implicitly[Zero[V2]].zero _))

  def createName = s"""mrpf-mstr-$n"""
}

/**
  * A composition MapReduce class
  *
  * @tparam T  the input type of the MapReduce function: T may be V1 for a first stage, or (K1,V1) for a subsequent stage.
  * @tparam K2 intermediate key type
  * @tparam V2 intermediate value type
  * @tparam K3 output key type
  * @tparam V3 output value type
  * @param f the mapper function which takes a V1 instance and creates a key-value tuple of type (K2,W)
  * @param g the reducer function which combines two values (an V2 and a W) into one V2
  */
case class MapReduceComposed[T, K2, V2, K3, V3](f: MapReduce[T, K2, V2], g: ASync[Seq[(K2, V2)], Map[K3, V3]])(implicit val ec: ExecutionContext) extends MapReduce[T, K3, V3] {
  def apply(ts: Seq[T]): Future[Map[K3, V3]] = for (v2K2m: Map[K2, V2] <- f(ts); v3K3m: Map[K3, V3] <- g(v2K2m.toSeq)) yield v3K3m
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
  * @author scalaprof
  * @tparam T  the input type of the MapReduce function: T may be V1 for a first stage, or (K1,V1) for a subsequent stage.
  * @tparam K2 intermediate key type
  * @tparam V2 intermediate value type
  * @param config  an instance of Config which defines a suitable configuration
  * @param system  the actor system
  * @param timeout the value of timeout to be used
  */
abstract class MapReduce_LoggingBase[T, K2, V2](config: Config, system: ActorSystem)(implicit timeout: Timeout) extends MapReduce_Base[T, K2, V2](system)(timeout) {
  private val exceptionStack = config.getBoolean("exceptionStack")

  def logException(m: String, x: Throwable): Unit = if (exceptionStack) system.log.error(x, m) else system.log.warning(s"$m: ${x.getLocalizedMessage}")
}

/**
  * An abstract base class for MapReduce classes (other than MapReduceComposed).
  */
abstract class MapReduce_Base[T, K, V](system: ActorSystem)(implicit timeout: Timeout) extends MapReduce[T, K, V] {
  self =>
  implicit def ec: ExecutionContextExecutor = system.dispatcher

  private val master = system.actorOf(createProps, createName)

  def apply(ts: Seq[T]): Future[Map[K, V]] = {
    // Note: currently, we ignore the value of report but we could pass back a tuple that includes ok and the resulting map
    for (vKr <- master.ask(ts).mapTo[Response[K, V]]; _ = report(vKr)) yield vKr.right
  }

  def createProps: Props

  def createName: String

  def report(vKr: Response[K, V]): Boolean = {
    for ((k, x) <- vKr.left) logException(s"exception thrown (but forgiven) for key $k", x)
    vKr.size == 0
  }

  def logException(m: String, x: Throwable): Unit
}

/**
  * Type-class Zero is used to add behavior of initialization (or zeroing) of X.
  *
  * @tparam X the type which we want to create a zero value for.
  */
trait Zero[X] {
  /**
    * Method to create a zero/empty/nothing value of X
    *
    * @return an X which is zero or empty
    */
  def zero: X
}
