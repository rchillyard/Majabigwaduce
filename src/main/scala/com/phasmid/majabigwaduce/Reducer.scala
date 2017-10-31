package com.phasmid.majabigwaduce

import com.phasmid.majabigwaduce.FP._

import scala.util._

/**
  * This actor performs the reduce operation on the received sequence of W objects,
  * resulting in (ideally) a V2 object.
  * The incoming "Intermediate" message combines both the current K2 key and the sequence ws of W objects.
  * The reply message is a tuple of (K2,Either[Throwable,V2])
  *
  * Intermediate is a convenience incoming message wrapper. It has the advantage of not suffering type erasure.
  *
  * @author scalaprof
  * @tparam K2 key type
  * @tparam W  value type
  * @tparam V2 the aggregation of W objects (in this form, must be super-type of W)
  * @param g a function which takes a V2 (the accumulator) and a W (the value) and combines them into a V2
  */
class Reducer[K2, W, V2 >: W](g: (V2, W) => V2) extends ReducerBase[K2, W, V2] {
  def getValue(ws: Seq[W]): V2 = ws.reduceLeft(g)
}

/**
  * This actor performs the reduce operation on the received sequence of V2 objects,
  * resulting in an V2 object.
  * The incoming "Intermediate" message combines both the current K2 key and the sequence ws of W objects.
  * The reply message is a tuple of (K2,Either[Throwable,V2])
  *
  * Intermediate is a convenience incoming message wrapper. It has the advantage of not suffering type erasure.
  *
  * @author scalaprof
  * @tparam K2 key type
  * @tparam W  value type
  * @tparam V2 the aggregation of W objects
  * @param g a function which takes a V2 (the accumulator) and a W (the value) and combines them into a V2
  * @param z a function which provides an initial value for V2 (this allows us to use Fold rather than Reduce methods)
  */
class Reducer_Fold[K2, W, V2](g: (V2, W) => V2, z: => V2) extends ReducerBase[K2, W, V2] {
  def getValue(ws: Seq[W]): V2 = ws.foldLeft(z)(g)
}

/**
  * Base class to implement different types of reducer.
  *
  * Note that logging the actual values received in the incoming message can be VERY verbose.
  * It is therefore recommended practice to log the values as they pass through the reducer function (g, in the sub-classes) which is
  * under the control of the application.
  * Therefore the call to maybeLog is commented out.
  *
  * @tparam K2 key type
  * @tparam W  value type
  * @tparam V2 the aggregation of W objects
  */
abstract class ReducerBase[K2, W, V2] extends MapReduceActor {

  override def receive: PartialFunction[Any, Unit] = {
    case i: Intermediate[K2, W] =>
      log.info(s"received $i")
      sender ! (i.k2, sequence(Try(getValue(i.ws))))
    case q =>
      super.receive(q)
  }

  def getValue(ws: Seq[W]): V2
}

case class Intermediate[K2, W](k2: K2, ws: Seq[W]) {
  override def toString = s"Intermediate: with k2=$k2 and ${ws.size} elements"
}
