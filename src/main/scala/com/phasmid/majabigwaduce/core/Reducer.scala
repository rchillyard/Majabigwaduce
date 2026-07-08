/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce.core

import com.phasmid.majabigwaduce.core.FP.*

import scala.util.*

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
class Reducer[K2, W, V2 >: W](g: (V2, W) => V2) extends ReducerBase[K2, W, V2]:
  /**
   * Reduces a sequence of W objects into a single V2 object using the provided aggregation function.
   *
   * @param ws the sequence of W objects to be reduced
   * @return the result of reducing the sequence into a single V2 object
   */
  def getValue(ws: Seq[W]): V2 = ws.reduceLeft(g)

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
class Reducer_Fold[K2, W, V2](g: (V2, W) => V2, z: => V2) extends ReducerBase[K2, W, V2]:
  /**
   * Computes an aggregated value of type V2 by iteratively applying the fold operation on
   * the provided sequence of elements of type W, using the initial value z and the combine
   * function g.
   *
   * @param ws the sequence of elements of type W to be aggregated
   * @return the aggregated result of type V2
   */
  def getValue(ws: Seq[W]): V2 = ws.foldLeft(z)(g)

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
abstract class ReducerBase[K2, W, V2] extends MapReduceActor:

  /**
   * Handles incoming messages and processes them based on their type.
   *
   * When an `Intermediate` message is received, it logs the message, converts the intermediate values
   * to an `Either` type using the result of `getValue` applied to the `ws` field of the intermediate object,
   * and sends the resulting tuple `(k2, processedValue)` back to the sender.
   * Falls back to the superclass implementation for any unhandled messages.
   *
   * @return A partial function that maps incoming messages of type `Any` to unit-producing actions.
   */
  override def receive: PartialFunction[Any, Unit] = {
    case i: Intermediate[K2, W] @unchecked =>
      log.debug(s"Reducer received $i")
      sender() ! (i.k2, toEither(Try(getValue(i.ws))))
    case q =>
      super.receive(q)
  }

  /**
   * Aggregates a sequence of values of type `W` into a single value of type `V2`.
   *
   * @param ws the input sequence of elements of type `W` to be aggregated
   * @return the aggregated value of type `V2` computed from the input sequence
   */
  def getValue(ws: Seq[W]): V2

/**
 * Represents an intermediate result containing a key-value pair and a sequence of elements.
 *
 * @tparam K2 The type of the key.
 * @tparam W  The type of the elements in the sequence.
 * @param k2 The key of the intermediate result.
 * @param ws A sequence of elements associated with the key.
 */
case class Intermediate[K2, W](k2: K2, ws: Seq[W]):
  override def toString = s"Intermediate: with k2=$k2 and ${ws.size} elements"
