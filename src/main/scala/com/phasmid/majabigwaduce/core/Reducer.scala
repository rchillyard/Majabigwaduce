/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce.core

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import com.phasmid.majabigwaduce.core.FP.*
import org.slf4j.{Logger, LoggerFactory}

import scala.util.*

/**
 * This actor performs the reduce operation on the received sequence of W objects,
 * resulting in (ideally) a V2 object.
 * It handles a `Reduce` command combining both the current K2 key and the sequence ws of W objects.
 * The reply is a `ReduceResult[K2,V2]`.
 *
 * Note that logging the actual values received in the incoming message can be VERY verbose.
 * It is therefore recommended practice to log the values as they pass through the reducer function (g) which is
 * under the control of the application.
 */
sealed trait ReducerCommand[K2, W, V2]

/**
 * Command to reduce the sequence of W values associated with a key, replying to `replyTo` with a `ReduceResult`.
 *
 * NOTE: named DoReduce (not Reduce) to avoid clashing with the unrelated Reduce[K,T,S] class in MapReduce.scala.
 */
final case class DoReduce[K2, W, V2](i: Intermediate[K2, W], replyTo: ActorRef[ReduceResult[K2, V2]]) extends ReducerCommand[K2, W, V2]

/**
 * Command to stop this Reducer.
 */
final case class CloseReducer[K2, W, V2]() extends ReducerCommand[K2, W, V2]

/**
 * The response sent back from a Reducer.
 *
 * @param k2     the key of the intermediate result which was reduced.
 * @param result the aggregated value, or the Throwable resulting from a failed reduction.
 */
case class ReduceResult[K2, V2](k2: K2, result: Either[Throwable, V2])

/**
 * Builds a Behavior which handles ReducerCommand[K2,W,V2] by applying `getValue` to the sequence
 * of W values carried by each incoming Reduce command.
 */
private[core] object ReducerBase:
  def behavior[K2, W, V2](logger: Logger)(getValue: Seq[W] => V2): Behavior[ReducerCommand[K2, W, V2]] =
    MapReduceActor.withLifecycle(logger) { _ =>
      {
        case DoReduce(i, replyTo) =>
          logger.debug(s"Reducer received $i")
          replyTo ! ReduceResult(i.k2, toEither(Try(getValue(i.ws))))
          Behaviors.same
        case CloseReducer() =>
          Behaviors.stopped
      }
    }

/**
 * @author scalaprof
 * @param g a function which takes a V2 (the accumulator) and a W (the value) and combines them into a V2
 * @tparam K2 key type
 * @tparam W  value type
 * @tparam V2 the aggregation of W objects (in this form, must be super-type of W)
 */
object Reducer:
  private val logger = LoggerFactory.getLogger("com.phasmid.majabigwaduce.core.Reducer")

  def apply[K2, W, V2 >: W](g: (V2, W) => V2): Behavior[ReducerCommand[K2, W, V2]] =
    ReducerBase.behavior(logger)(ws => ws.reduceLeft(g))

/**
 * @author scalaprof
 * @param g a function which takes a V2 (the accumulator) and a W (the value) and combines them into a V2
 * @param z a function which provides an initial value for V2 (this allows us to use Fold rather than Reduce methods)
 * @tparam K2 key type
 * @tparam W  value type
 * @tparam V2 the aggregation of W objects
 */
object Reducer_Fold:
  private val logger = LoggerFactory.getLogger("com.phasmid.majabigwaduce.core.Reducer_Fold")

  def apply[K2, W, V2](g: (V2, W) => V2, z: => V2): Behavior[ReducerCommand[K2, W, V2]] =
    ReducerBase.behavior(logger)(ws => ws.foldLeft(z)(g))

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
