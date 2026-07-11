/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce.core

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{Behavior, PostStop}
import com.phasmid.majabigwaduce.core.FP.*
import org.slf4j.Logger

import scala.collection.mutable
import scala.util.*

/**
 * Shared lifecycle support for the actors in this package (Mapper, Reducer, Master).
 *
 * Akka Typed behaviors are composed, not inherited, so this replaces what was previously an
 * abstract Actor base class. `withLifecycle` wraps a command handler with the same debug-level
 * start/stop logging the old base class provided via preStart/postStop. Each actor's own command
 * handler recognizes its own protocol's Close case and returns Behaviors.stopped -- there is no
 * shared Close type across protocols, since each actor's command ADT is self-contained.
 */
object MapReduceActor:

  /**
   * Wraps a command handler with debug-level start/stop logging.
   *
   * `makeHandler` is invoked exactly once, when the actor starts, and is given the ActorContext
   * so it can perform any one-time setup (e.g. Master spawning its mapper and reducer children)
   * before returning the actual per-message handler. Actors with no such setup (Mapper, Reducer)
   * simply ignore the context and return a constant handler.
   *
   * @param logger      the SLF4J logger to log against.
   * @param makeHandler builds the command handler; the handler is responsible for recognizing
   *                    its own protocol's Close case and returning Behaviors.stopped.
   * @tparam T the command type handled by this behavior.
   * @return a Behavior[T].
   */
  def withLifecycle[T](logger: Logger)(makeHandler: ActorContext[T] => T => Behavior[T]): Behavior[T] =
    Behaviors.setup[T] { context =>
      logger.debug("is starting")
      Behaviors.receiveMessage[T](makeHandler(context))
        .receiveSignal {
          case (_, PostStop) =>
            logger.debug("has shut down")
            Behaviors.same
        }
    }

  /**
   * Logs a debug-level message if debug logging is enabled.
   * This method evaluates the provided message lazily to avoid
   * unnecessary computation if debug logging is not enabled.
   *
   * @param logger the SLF4J logger to log against.
   * @param w      the debug message to log.
   * @param z      the data or computation to include in the log message.
   */
  def maybeLog(logger: Logger, w: String, z: => Any): Unit = if logger.isDebugEnabled then logger.debug(w, z)

/**
 * Cleans a sequence of input data that may contain exceptions. The main purpose is to organize
 * the valid results by their keys and separate out any encountered exceptions. Used by Mapper to
 * build its response.
 */
object CleanerCollector:
  /**
   * Method to clean exceptions from the input, and collect the results together, returning appropriate output.
   *
   * @param kWys the input of type Seq of Try of (K, W).
   * @return the output of type (Map[K, Seq of W], Seq of Throwable).
   */
  def cleanAndCollect[K, W](kWys: Seq[Try[(K, W)]]): (Map[K, Seq[W]], Seq[Throwable]) =
    val kWsm = mutable.LinkedHashMap[K, Seq[W]]() // mutable
    val xs = mutable.ListBuffer[Throwable]() // mutable
    for (kWy <- kWys)
      toEither(kWy) match
        case Right((k, w)) => kWsm put(k, w +: kWsm.getOrElse(k, Nil))
        case Left(x) => x +=: xs

    (kWsm.toMap, xs.toSeq)

/**
 * Represents an exception that occurs during MapReduce operations.
 *
 * @param context A string describing the context or scenario where the exception was raised.
 * @param x       The underlying cause of the exception, provided as a throwable.
 */
case class MapReduceException(context: String, x: Throwable) extends Throwable(context, x)

object MapReduceException:
  def apply(context: String): MapReduceException =
    MapReduceException(context, null)
