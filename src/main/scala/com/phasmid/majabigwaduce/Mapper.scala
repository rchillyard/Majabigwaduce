/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce

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
  * It replies with a Try[Map[K2,Seq[W] which will be a Failure if any of the mappings fail.
  *
  * Incoming is a convenience incoming message wrapper. It has the advantage of not suffering type erasure.
  *
  * This mapper is strict in the sense that if there are any mapping exceptions, then the mapper as a whole fails
  * and returns an empty map (after logging an error). However, you can change this behavior in the configuration
  * file by setting forgiving=true.
  *
  * The normal reply is in the form of: Try[Map[K2,Seq[W] but, if the forgiving form of the mapper is used,
  * then the reply is in the form of a tuple: (Map[K2,Seq[W], Seq[Throwable])
  *
  * Note that logging the actual values received in the incoming message can be VERY verbose.
  * It is therefore recommended practice to log the values as they pass through the mapper function (f) which is
  * under the control of the application.
  * Therefore the call to maybeLog is commented out.
  *
  * @author scalaprof
  * @param f function to convert a (K1,V1) pair into a Try[(K2,V2)]
  * @tparam K1 (input) key type (may be Unit)
  * @tparam K2 (output) key type
  * @tparam V1 (input) value type
  * @tparam W  (output) value type
  *
  */
class Mapper[K1, V1, K2, W](f: (K1, V1) => Try[(K2, W)]) extends MapReduceActor with Responder[K2, W] with CleanerCollector[K2, W] {

  override def receive: PartialFunction[Any, Unit] = {
    case i: KeyValueSeq[K1, V1] =>
      log.info(s"Mapper received $i")
      // CONSIDER using a form of groupBy to perform this operation
      val wk2ts: Seq[Try[(K2, W)]] = for ((k1, v1) <- i.m) yield f(k1, v1)
      sendReply(sender, prepareResponse[Map[K2, Seq[W]]](wk2ts))
    case q =>
      super.receive(q)
  }


  //  override def processExceptions[Y: ClassTag](x: Seq[Try[(K2, W)]]): (Y, Seq[Throwable]) = ???
}

/**
  * This sub-class of Mapper is more forgiving (and retains any exceptions thrown).
  * The reply is in the form of a tuple: (Map[K2,Seq of W],Seq[Throwable])
  *
  * @author scalaprof
  * @param f function to convert a (K1,V1) pair into a Try[(K2,V2)]
  * @tparam K1 (input) key type (may be Unit)
  * @tparam K2 (output) key type
  * @tparam V1 (input) value type
  * @tparam W  (output) value type
  */
class Mapper_Forgiving[K1, V1, K2, W](f: (K1, V1) => Try[(K2, W)]) extends Mapper[K1, V1, K2, W](f) {
  override val isStrict: Boolean = false
}

// CONSIDER eliminating this case class and simply using the KeyValueSeq object to build a Seq[(K, V])]
case class KeyValueSeq[K, V](m: Seq[(K, V)]) {
  override def toString = s"Incoming: with ${m.size} elements"
}

object KeyValueSeq {
  def sequence[K, V](vs: Seq[V]): KeyValueSeq[K, V] = KeyValueSeq((vs zip LazyList.continually(null.asInstanceOf[K])).map(_.swap))

  def map[K, V](vKm: Map[K, V]): KeyValueSeq[K, V] = KeyValueSeq(vKm.toSeq)
}


