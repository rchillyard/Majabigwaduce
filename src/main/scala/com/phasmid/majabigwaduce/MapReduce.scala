package com.phasmid.majabigwaduce

import scala.util._
import scala.concurrent._
import scala.concurrent.duration._
import akka.actor.{ActorSystem, Props, ActorRef}
import akka.util.Timeout
import akka.pattern.ask
import com.typesafe.config.Config

/**
 * MapReduce is a trait, with case classes, which implements a functional API for the map-reduce classes in this package.
 * 
 * @author scalaprof
 * 
 * CONSIDER renumbering the K, V types so that the input to the first stage is Seq[V0] and the output of the nth stage is Map[Kn,Vn]
 *
 * @param <T> the input type of the MapReduce function: T may be V1 for a first stage, or (K1,V1) for a subsequent stage.
 * @param <K2> the key type of the returned map
 * @param <V2> the value type of the returned map
 */
trait MapReduce[T,K2,V2] extends Function1[Seq[T],Future[Map[K2,V2]]] {
    /**
     * compose this MapReduce object with mr, yielding a new MapReduce object.
     * @param mr the other MapReduce object
     * @return a new MapReduceComposed object
     */
    def compose[K3,V3](mr: MapReduce[(K2,V2),K3,V3]): MapReduce[T,K3,V3] = MapReduceComposed(this,mr)
    /**
     * terminate this MapReduce object with r, a reducer which yields a simple value
     * @param r the Reduce object
     * @param executionContext (implicit)
     * @param <S> the return type
     * @return a Future of an object of type S (for sum, or sigma).
     */
    def compose[S>:V2](r: Reduce[V2,S])(implicit executionContext: ExecutionContext): Function1[Seq[T],Future[S]]= { ts => for (v2K2m <- apply(ts); s = r.apply(v2K2m)) yield s }
    /**
     * @return a suitable execution context
     */
    def ec: ExecutionContext
}

/**
 * A first-stage MapReduce class where the result type V2 is a super-type of the intermediate type W
 *
 * @param <V1> input value type
 * @param <K2> output key type
 * @param <W> intermediate type
 * @param <V2> output value type (super-type of W)
 */
case class MapReduceFirst[V1,K2,W,V2>:W](f: (Unit,V1)=>(K2,W), g: (V2,W)=>V2)(implicit config: Config, system: ActorSystem, timeout: Timeout) extends MapReduce_LoggingBase[V1,K2,V2](config,system)(timeout) {
  def createProps= Props(new Master_First(config,f,g))
  def createName = s"""mrf-mstr"""
}

/**
 * A later-stage MapReduce class where the result type V2 is a super-type of the intermediate type W
 *
 * @param <K1> input key type
 * @param <V1> input value type
 * @param <K2> output key type
 * @param <W> intermediate type
 * @param <V2> output value type (super-type of W)
 */
case class MapReducePipe[K1,V1,K2,W,V2>:W](f: (K1,V1)=>(K2,W), g: (V2,W)=>V2, n: Int)(implicit config: Config, system: ActorSystem, timeout: Timeout) extends MapReduce_LoggingBase[(K1,V1),K2,V2](config,system)(timeout) {
  def createProps = Props(new Master(config,f,g))  
  def createName = s"""mrp-mstr-$n"""
}

/**
 * A first-stage MapReduce class
 *
 * @param <V1> input value type
 * @param <K2> output key type
 * @param <W> intermediate type
 * @param <V2> output value type
 */
case class MapReduceFirstFold[V1,K2,W,V2](f: (Unit,V1)=>(K2,W), g: (V2,W)=>V2, z: ()=>V2)(implicit config: Config, system: ActorSystem, timeout: Timeout) extends MapReduce_LoggingBase[V1,K2,V2](config,system)(timeout) {
  def createProps = Props(new Master_First_Fold(config,f,g,z))  
  def createName = s"""mrff-mstr"""
}

/**
 * A later-stage MapReduce class
 *
 * @param <K1> input key type
 * @param <V1> input value type
 * @param <K2> output key type
 * @param <W> intermediate type
 * @param <V2> output value type
 */
case class MapReducePipeFold[K1,V1,K2,W,V2](f: (K1,V1)=>(K2,W), g: (V2,W)=>V2, z: ()=>V2, n: Int)(implicit config: Config, system: ActorSystem, timeout: Timeout) extends MapReduce_LoggingBase[(K1,V1),K2,V2](config,system)(timeout) {
  def createProps = Props(new Master_Fold(config,f,g,z))  
  def createName = s"""mrpf-mstr-$n"""
}

/**
 * A composition MapReduce class
 *
 * @param <T> the input type of the MapReduce function: T may be V1 for a first stage, or (K1,V1) for a subsequent stage.
 * @param <K2> intermediate key type
 * @param <V2> intermediate value type
 * @param <K3> output key type
 * @param <V3> output value type
 */
case class MapReduceComposed[T,K2,V2,K3,V3](f: MapReduce[T,K2,V2], g: MapReduce[(K2,V2),K3,V3]) extends MapReduce[T,K3,V3] { 
  implicit val executionContext = f.ec
  def ec = executionContext
  def apply(ts: Seq[T]) = for (v2K2m <- f.apply(ts); v3K3m <- g.apply(v2K2m.toSeq)) yield v3K3m
}

/**
 * A reduce function which can be composed (on the right) with a MapReduce object.
 *
 * @param <V1>
 * @param <S>
 */
case class Reduce[V1, S>:V1](f: (S,V1)=>S) extends Function1[Map[_,V1],S] {
  def apply(m: Map[_,V1]) = m.values reduceLeft f
}

abstract class MapReduce_LoggingBase[T, K2, V2](config: Config, system: ActorSystem)(implicit timeout: Timeout) extends MapReduce_Base[T,K2,V2](system)(timeout) { 
  val exceptionStack = config.getBoolean("exceptionStack")
  def logException(m: String, x: Throwable): Unit = if (exceptionStack) system.log.error(x,m) else system.log.warning(s"$m: ${x.getLocalizedMessage}")
}

/**
 * An abstract base class for MapReduce classes (other than MapReduceComposed).
 */
abstract class MapReduce_Base[T, K2, V2](system: ActorSystem)(implicit timeout: Timeout) extends MapReduce[T,K2,V2] { self =>
  implicit def ec = system.dispatcher
	val master = system.actorOf(createProps, createName)
  def apply(ts: Seq[T]) = {
    // Note: currently, we ignore the value of ok but we could pass back a tuple that includes ok and the resulting map
    for (v2K2r <- master.ask(ts).mapTo[Response[K2,V2]]; ok = report(v2K2r)) yield v2K2r.right
  }
  def createProps: Props
  def createName: String
  def report(v2K2r: Response[K2,V2]): Boolean = {
     for ((k,x) <- v2K2r.left) logException(s"exception thrown (but forgiven) for key $k",x)
     v2K2r.size==0
  }
  def logException(m: String, x: Throwable): Unit
}
