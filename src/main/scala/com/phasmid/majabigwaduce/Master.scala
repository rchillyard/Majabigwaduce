package com.phasmid.majabigwaduce

import scala.collection.mutable.{HashMap,MutableList}
import scala.concurrent.{Future,Await}
import scala.concurrent.duration._
import scala.util._
import akka.actor.{ ActorSystem, Props, ActorRef }
//import akka.event.LoggingAdapter
import akka.pattern.ask
import akka.util.Timeout
import java.net.URL
import scala.concurrent._
import com.typesafe.config.Config

class Master[K1, V1, K2, W, V2>:W](config: Config, f: (K1,V1)=>(K2,W), g: (V2,W)=>V2) extends MasterBase[K1, V1, K2, W, V2](config, f, g, Master.zero) with ByReduce[K1, V1, K2, W, V2] 

class Master_Fold[K1, V1, K2, W, V2](config: Config, f: (K1,V1)=>(K2,W), g: (V2,W)=>V2, z: ()=>V2) extends MasterBase[K1, V1, K2, W, V2](config, f, g, z) with ByFold[K1, V1, K2, W, V2]

class Master_First[V1, K2, W, V2>:W](config: Config, f: (Unit,V1)=>(K2,W), g: (V2,W)=>V2) extends MasterBaseFirst[V1, K2, W, V2](config, f, g, Master.zero) with ByReduce[Unit, V1, K2, W, V2]

class Master_First_Fold[V1, K2, W, V2](config: Config, f: (Unit,V1)=>(K2,W), g: (V2,W)=>V2, z: ()=>V2) extends MasterBaseFirst[V1, K2, W, V2](config, f, g, z) with ByFold[Unit, V1, K2, W, V2]

trait ByReduce[K1, V1, K2, W, V2>:W] {
    def mapperProps(f: (K1,V1)=>(K2,W), config: Config): Props = 
      if (Master.isForgiving(config)) Props.create(classOf[Mapper_Forgiving[K1,V1,K2,W]], f) else Props.create(classOf[Mapper[K1,V1,K2,W]], f)
    def reducerProps(f: (K1,V1)=>(K2,W), g: (V2,W)=>V2, z: ()=>V2): Props = Props.create(classOf[Reducer[K2,W,V2]], g)
}
trait ByFold[K1, V1, K2, W, V2]{
    def mapperProps(f: (K1,V1)=>(K2,W), config: Config): Props =
      if (Master.isForgiving(config: Config)) Props.create(classOf[Mapper_Forgiving[K1,V1,K2,W]], f) else Props.create(classOf[Mapper[K1,V1,K2,W]], f)
    def reducerProps(f: (K1,V1)=>(K2,W), g: (V2,W)=>V2, z: ()=>V2): Props = Props.create(classOf[Reducer_Fold[K2,W,V2]], g, z)
}

/**
 * Abstract class MasterBaseFirst
 * 
 * This version of the MasterBase class (which it extends) take a different type of message: to wit, a Seq[V1].
 * That is to say, there is no K1 type.
 * 
 * @author scalaprof
 *
 * @param <V1> input type: the message which this actor responds to is of type Seq[V1].
 * @param <K2> key type: mapper groups things by this key and reducer processes said groups.
 * @param <W> transitional type -- used internally
 * @param <V2> output type: the message which is sent on completion to the sender is of type Response[K2,V2]
 * 
 * @param f the mapper function which takes a V1 and creates a key-value tuple of type (K2,W)
 * @param g the reducer function which combines two values (an V2 and a W) into one V2
 */
abstract class MasterBaseFirst[V1, K2, W, V2](config: Config, f: (Unit,V1)=>(K2,W), g: (V2,W)=>V2, z: ()=>V2) extends MasterBase[Unit, V1, K2, W, V2](config, f, g, z) {
  import context.dispatcher
  override def receive = {
    case v1s: Seq[V1] =>
      log.info(s"received Seq[V1]: with ${v1s.length} elements")
      maybeLog("received {}",v1s)
      val caller = sender
      doMapReduce(Incoming.sequence[Unit,V1](v1s)).onComplete {
        case Success(wXeK2m) => caller ! Response(wXeK2m)
        case Failure(x) => caller ! akka.actor.Status.Failure(x)
      }
    case q =>
      super.receive(q)
  }
}

/**
 * @author scalaprof
 *
 * @param <K1> key type: input may be organized by this key (may be "Unit").
 * @param <V1> input type: the message which this actor responds to is of type Map[K1,V1]
 * @param <K2> key type: mapper groups things by this key and reducer processes said groups.
 * @param <W> transitional type -- used internally
 * @param <V2> output type: the message which is sent on completion to the sender is of type Response[K2,V2]
 * 
 * @param f the mapper function which takes a K1,V1 pair and creates a key-value tuple of type (K2,W)
 * @param g the reducer function which combines two values (an V2 and a W) into one V2
 * @param z the zero (initializer) function which creates an "empty" V2.
 * @param n the stage number of this map-reduce stage.
 */
abstract class MasterBase[K1, V1, K2, W, V2](config: Config, f: (K1,V1)=>(K2,W), g: (V2,W)=>V2, z: ()=>V2) extends MapReduceActor {
  implicit val timeout = getTimeout(config.getString("timeout"))
  val mapper = context.actorOf(mapperProps(f,config), "mpr")
  val reducers = for (i <- 1 to config.getInt("reducers")) yield context.actorOf(reducerProps(f,g,z), s"rdcr-$i")
  import context.dispatcher
  
  if (Master.isForgiving(config)) log.info("setting forgiving mode")
  
  def mapperProps(f: (K1,V1)=>(K2,W), config: Config): Props
  def reducerProps(f: (K1,V1)=>(K2,W), g: (V2,W)=>V2, z: ()=>V2): Props
  
  // CONSIDER reworking this so that there is only one possible valid message: 
  // either in Map[] form of Seq[()] form. I don't really like having both
  override def receive = {
    case v1K1m: Map[K1,V1] =>
      log.info(s"received Map[K1,V1]: with ${v1K1m.size} elements")
      maybeLog("received: {}",v1K1m)
      val caller = sender
      doMapReduce(Incoming.map[K1,V1](v1K1m)).onComplete {
        case Success(v2XeK2m) =>
          maybeLog("response: {}", v2XeK2m)
          caller ! Response(v2XeK2m)
        case Failure(x) =>
          log.error(x,s"no response--failure")
          caller ! akka.actor.Status.Failure(x)
      }
    case v1s: Seq[(K1,V1)] @unchecked =>
      log.info(s"received Seq[(K1,V1)]: with ${v1s.length} elements")
      maybeLog("received: {}",v1s)
      val caller = sender
      doMapReduce(Incoming[K1,V1](v1s)).onComplete {
        case Success(v2XeK2m) => caller ! Response(v2XeK2m)
        case Failure(x) => caller ! akka.actor.Status.Failure(x)
      }
    case q =>
      super.receive(q)
  }
  
  def doMapReduce(i: Incoming[K1,V1]) = for {
      wsK2m <- doMap(i)
      z = maybeLog("shuffle: {}", wsK2m)
      v2XeK2m <- doDistributeReduceCollate(wsK2m)
    } yield v2XeK2m
    
  private def doMap(i: Incoming[K1,V1]): Future[Map[K2,Seq[W]]] = {
    val reply = (mapper ? i)
    if (Master.isForgiving(config: Config))
      reply.mapTo[(Map[K2,Seq[W]],Seq[Throwable])] map {
        _ match { case (wsK2m,xs) => for (x <- xs) log.error(x,"mapper exception"); wsK2m }
      }
    else {
      val wsK2mtf = reply.mapTo[Try[Map[K2,Seq[W]]]]
      Master.flatten(wsK2mtf)
    }
  }

  private def doDistributeReduceCollate(wsK2m: Map[K2,Seq[W]]): Future[Map[K2,Either[Throwable,V2]]] = {
    if (wsK2m.size==0) log.warning("mapper returned empty map"+(if(Master.isForgiving(config: Config))""else": see log for problem and consider using Mapper_Forgiving instead"))
    maybeLog("doDistributeReduceCollate: {}", wsK2m)
    val rs = Stream.continually(reducers.toStream).flatten
    val wsK2s = for ((k2,ws) <- wsK2m.toSeq) yield (k2,ws)
    val v2XeK2fs = for (((k2,ws),a) <- (wsK2s zip rs)) yield (a ? Intermediate(k2,ws)).mapTo[(K2,Either[Throwable,V2])]
    for (wXeK2s <- Future.sequence(v2XeK2fs)) yield wXeK2s.toMap
  }
  
}

case class Response[K,V](left: Map[K,Throwable], right: Map[K,V]) {
  override def toString = s"left: $left; right: $right"
  def size = right.size
}

object Response {
  def apply[K,V](vXeKm: Map[K,Either[Throwable,V]]) = {
    val t = Master.toMap(Master.sequenceLeftRight(vXeKm))
    new Response(t._1,t._2)
  }
}

object Master {
  def zero[V]() = 0.asInstanceOf[V]
  
  // CONSIDER moving all these to MonadOps
  /**
   * Method sequence which applied to a Try[X] returns an Either[Throwable,X].
   * @param xt : Try[X]
   * @return : Either[Throwable,X]
   */
  def sequence[X](xt: Try[X]): Either[Throwable,X] = xt match { case Success(s) => Right(s); case Failure(e) => Left(e) }
  /**
   * Method sequence which, applied to a Seq[Try[X]], returns a Try[Seq[X]]
   * @param xts : Seq[Try[X]]
   * @return : Try[Seq[X]]
   */
  def sequence[X](xts : Seq[Try[X]]) : Try[Seq[X]] = (Try(Seq[X]()) /: xts) { (xst, xt) => for (xs <- xst; x <- xt ) yield xs :+ x }

  def flatten[X](xyf : Future[Try[X]])(implicit executor: ExecutionContext): Future[X] = {
		def convert[W](wy: Try[W]): Future[W]  = {
		  val wp = Promise[W]
			wy match {
			  case Success(y) => wp complete Success(y)
			  case Failure(e) => wp complete Failure(e)
			  }
		  wp.future
		}
    for (xy <- xyf; x <- convert(xy)) yield x
  }

  def sequence[K, V, X](vXeKm: Map[K,Either[X,V]]): (Map[K,X],Map[K,V]) = toMap(sequenceLeftRight(vXeKm))
  def sequenceLeft[K, V, X](vXeKs: Seq[(K,Either[X,V])]): Seq[(K,X)] = for ((k,e) <- vXeKs) yield (k,e.left.get)
  def sequenceRight[K, V, X](vXeKs: Seq[(K,Either[X,V])]): Seq[(K,V)] = for ((k,e) <- vXeKs) yield (k,e.right.get)
  def tupleMap[L1,L2,R1,R2](fl: L1=>L2, fr: R1=>R2)(t: (L1,R1)): (L2,R2) = (fl(t._1),fr(t._2))
  def partition[K, V, X](vXeKm: Map[K,Either[X,V]]): (Seq[(K,Either[X,V])],Seq[(K,Either[X,V])]) = vXeKm.toSeq.partition({case (k,v) => v.isLeft})
  def toMap[K, V, X](t: (Seq[(K,X)],Seq[(K,V)])): (Map[K,X],Map[K,V]) = (t._1.toMap,t._2.toMap)
  def sequenceLeftRight[K, V, X](vXeKm: Map[K,Either[X,V]]): (Seq[(K,X)],Seq[(K,V)]) = tupleMap[Seq[(K,Either[X,V])],Seq[(K,X)],Seq[(K,Either[X,V])],Seq[(K,V)]](sequenceLeft,sequenceRight)(partition(vXeKm))
  def isForgiving(config: Config) = config.getBoolean("forgiving")
}
