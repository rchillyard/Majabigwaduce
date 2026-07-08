/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce.core

import akka.actor.{Actor, ActorLogging, ActorRef, Status}
import akka.util.Timeout
import com.phasmid.majabigwaduce.core.FP.*

import scala.collection.mutable
import scala.concurrent.duration.*
import scala.reflect.ClassTag
import scala.util.*

abstract class MapReduceActor extends Actor with ActorLogging with AutoCloseable:
  /**
   * Invoked when the actor is about to start. This method initializes any necessary
   * resources or setups required before the actor begins processing messages.
   * It logs a debug-level message indicating that the actor is starting, and
   * calls the `preStart` method of the superclass for additional initialization.
   *
   * @return Unit, as this method performs setup operations without returning a value.
   */
  override def preStart(): Unit =
    log.debug("is starting")
    super.preStart()

  /**
   * Invoked after the actor has been stopped. This method is typically used to perform
   * any necessary cleanup tasks, such as releasing non-actor resources or logging the
   * completion of the actor's lifecycle.
   *
   * @return Unit, as this method performs cleanup operations without returning a value.
   */
  override def postStop(): Unit = {
    super.postStop()
    log.debug("has shut down")
  }

  /**
   * Handles incoming messages. It responds to specific messages or logs a warning
   * for unknown message types. Specifically:
   * - When receiving the `Close` message, it invokes the `close` method to release resources
   *   and stops the actor.
   * - For any other message, it logs a warning indicating the unrecognized message type.
   *   TESTME
   *
   * @return A partial function that defines the actor's behavior for each received message.
   */
  override def receive: PartialFunction[Any, Unit] =
    case Close =>
      close()
      context stop self
    case q =>
      log.warning(s"received unknown message type: ${q.getClass}")

  /**
    * This method takes a response which is a Try[Any] and sends it to the caller according to whether it is a success or failure.
    *
    * @param caller   the actor which requested the response.
    * @param response the response wrapped in Try.
    */
  def sendReply(caller: ActorRef, response: Try[Any]): Unit =
    caller ! (response match
      case Success(x) => x
      case Failure(x) => Status.Failure(x)
      )

  /**
   * Logs a debug-level message if debug logging is enabled.
   * This method evaluates the provided message lazily to avoid
   * unnecessary computation if debug logging is not enabled.
   *
   * @param w The debug message to log.
   * @param z The data or computation to include in the log message.
   *          This parameter is lazily evaluated.
   *
   * @return Unit, as this method performs logging and does not produce
   *         a result.
   */
  def maybeLog(w: String, z: => Any): Unit = if (log.isDebugEnabled) then log.debug(w, z)

  /**
   * Parses the given timeout string and converts it into a `Timeout` object.
   * The timeout string should specify a duration and its unit (e.g., "10 seconds").
   * If the input format is invalid, a default timeout of 10 seconds is returned.
   * Logs the resolved timeout value at the debug level.
   * TODO resolve duplicate code fragment
   *
   * @param t The timeout string specifying the duration and time unit.
   * @return A `Timeout` instance based on the provided string, or a default value if the string is invalid.
   */
  def getTimeout(t: String): Timeout =
    val durationR = """(\d+)\s*(\w+)""".r
    val timeout = t match
      case durationR(n, s) => new Timeout(FiniteDuration(n.toLong, s))
      case _ => Timeout(10.seconds)

    log.debug(s"setting timeout to: $timeout")
    timeout

  /**
   * Closes any non-actor resources associated with this component.
   * Actor resources are automatically managed and do not require manual closure.
   *
   * @return Unit, as this method performs cleanup operations without returning a value.
   */
  def close(): Unit = {}

trait Responder[K, W] extends CleanerCollector[K, W]:

  /**
    *
    * @return a value regarding whether or not this Responder will be strict about exceptions, or else forgiving.
    */
  val isStrict: Boolean = true

  /**
    * Method to prepare a response to a query of a particular form.
    *
    * The first part of the returned tuple (Y) is the payload.
    * The second part of the returned tuple (Seq[Throwable]) is a list of any exceptions thrown while evaluating the response.
    *
    * @param wKys a Seq of Try of Tuple of (K2,W).
    * @tparam Y the response type when successful.
    * @return a Try of Tuple of (Y, Seq[Throwable]).
   */
  def prepareResponse[Y: ClassTag](wKys: Seq[Try[(K, W)]]): Try[(Y, Seq[Throwable])] =
    val (kWsm, xs) = cleanAndCollect(wKys)
    if isStrict && xs.nonEmpty
    then Failure(xs.head)
    else kWsm match
      case y: Y =>
        Success(y -> xs)
      case _ =>
        Failure(MapReduceException(s"${kWsm.getClass} did not match expected type: ${summon[ClassTag[Y]]}"))

/**
 * A trait for collecting and cleaning a sequence of input data that may contain exceptions. The main purpose
 * is to organize the valid results by their keys and separate out any encountered exceptions.
 *
 * @tparam K the type of the key in the key-value data structure.
 * @tparam W the type of the value associated with the key.
 */
trait CleanerCollector[K, W]:
  /**
    * Method to clean exceptions from the input, and collect the results together, returning appropriate output.
    *
    * NOTE: this should be implemented inside an Actor to help retain referential transparency.
    *
    * @param kWys the input of type Seq of Try of (K, W).
    * @return the output of type (Map[K, Seq of W], Seq of Throwable).
   */
  def cleanAndCollect(kWys: Seq[Try[(K, W)]]): (Map[K, Seq[W]], Seq[Throwable]) =
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
 * This exception encapsulates a specific context in which the error occurred,
 * along with the original throwable cause of the failure, if provided.
 *
 * @param context A string describing the context or scenario where the exception was raised.
 * @param x       The underlying cause of the exception, provided as a throwable.
 */
case class MapReduceException(context: String, x: Throwable) extends Throwable(context, x)

object MapReduceException:
  def apply(context: String): MapReduceException =
    MapReduceException(context, null)

/**
  * CONSIDER Don't think we really need this close mechanism. Akka does everything for us.
  */
object Close