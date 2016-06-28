package com.phasmid.majabigwaduce

import scala.collection.mutable
import scala.util._

/**
  * The purpose of this mapper is to convert a sequence of objects into several sequences, each of which is
  * associated with a key. It must be possible to do further processing (the reduce phase) on each of these
  * resulting sequences independently (and, thus in parallel).
  * Furthermore, the mapping function should try, when possible, to divide the input sequence into a number
  * of more or less equally lengthy sequences.
  *
  * The mapper is an actor whose constructor takes a function f which converts a (K1,V1) into a (K2,W).
  * The receive method recognizes an Incoming[K1,V1] as a message.
  * It replies with a Try[Map[K2,Seq[W]]] which will be a Failure if any of the mappings fail.
  *
  * Incoming is a convenience incoming message wrapper. It has the advantage of not suffering type erasure.
  *
  * This mapper is strict in the sense that if there are any mapping exceptions, then the mapper as a whole fails
  * and returns an empty map (after logging an error). However, you can change this behavior in the configuration
  * file by setting forgiving=true.
  *
  * The normal reply is in the form of: Try[Map[K2,Seq[W]]] but, if the forgiving form of the mapper is used,
  * then the reply is in the form of a tuple: (Map[K2,Seq[W]], Seq[Throwable])
  *
  * Note that logging the actual values received in the incoming message can be VERY verbose.
  * It is therefore recommended practice to log the values as they pass through the mapper function (f) which is
  * under the control of the application.
  * Therefore the call to maybeLog is commented out.
  *
  * @author scalaprof
  * @tparam K1 (input) key type (may be Unit)
  * @tparam K2 (output) key type
  * @tparam V1 (input) value type
  * @tparam W  (output) value type
  *
  */
class Mapper[K1, V1, K2, W](f: (K1, V1) => (K2, W)) extends MapReduceActor {

  override def receive = {
    case i: Incoming[K1, V1] =>
      log.info(s"received $i")
      //      maybeLog(s"with map {}", i.m)
      // CONSIDER using a form of groupBy to perform this operation
      val wk2ts = for ((k1, v1) <- i.m) yield Try(f(k1, v1))
      sender ! prepareReply(wk2ts)
    case q =>
      super.receive(q)
  }

  def prepareReply(wk2ts: Seq[Try[(K2, W)]]): Any = prepareReplyAsTry(wk2ts)

  private def prepareReplyAsTry(wk2ts: Seq[Try[(K2, W)]]): Try[Map[K2, Seq[W]]] =
    Master.sequence(wk2ts) match {
      case Success(wk2s) =>
        val wsK2m = mutable.HashMap[K2, Seq[W]]() // mutable
        for ((k2, w) <- wk2s) wsK2m put(k2, w +: wsK2m.getOrElse(k2, Nil))
        Success(wsK2m.toMap)
      case Failure(x) =>
        log.warning(s"prepareReply: exception noted and returned as failure: ${x.getLocalizedMessage}")
        Failure(MapReduceException(s"prepareReply (see previously in log)", x))
    }
}

/**
  * This sub-class of Mapper is more forgiving (and retains any exceptions thrown).
  * The reply is in the form of a tuple: (Map[K2,W],Seq[Throwable])
  *
  * @author scalaprof
  * @tparam K1 (input) key type (may be Unit)
  * @tparam K2 (output) key type
  * @tparam V1 (input) value type
  * @tparam W  (output) value type
  */
class Mapper_Forgiving[K1, V1, K2, W](f: (K1, V1) => (K2, W)) extends Mapper[K1, V1, K2, W](f) {

  override def prepareReply(wk2ts: Seq[Try[(K2, W)]]): Any = prepareReplyAsTuple(wk2ts)

  private def prepareReplyAsTuple(wk2ts: Seq[Try[(K2, W)]]): (Map[K2, Seq[W]], Seq[Throwable]) = {
    val wsK2m = mutable.HashMap[K2, Seq[W]]() // mutable
    val xs = Seq[Throwable]() // mutable
    for (wk2t <- wk2ts; wk2e = Master.sequence(wk2t))
      wk2e match {
        case Right((k2, w)) => wsK2m put(k2, w +: wsK2m.getOrElse(k2, Nil))
        case Left(x) => xs :+ x
      }
    (wsK2m.toMap, xs)
  }
}

case class Incoming[K, V](m: Seq[(K, V)]) {
  override def toString = s"Incoming: with ${m.size} elements"
}

object Incoming {
  def sequence[K, V](vs: Seq[V]): Incoming[K, V] = Incoming((vs zip Stream.continually(null.asInstanceOf[K])).map {
    _.swap
  })

  def map[K, V](vKm: Map[K, V]): Incoming[K, V] = Incoming(vKm.toSeq)
}

