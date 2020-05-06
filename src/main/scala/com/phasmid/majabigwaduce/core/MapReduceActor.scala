/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce.core

import akka.actor.{Actor, ActorLogging, ActorRef, Status}
import akka.util.Timeout
import com.phasmid.majabigwaduce.core.FP._

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.reflect.ClassTag
import scala.util._

abstract class MapReduceActor extends Actor with ActorLogging with AutoCloseable {
  override def preStart: Unit = {
    log.debug("is starting")
    super.preStart
  }

  override def postStop: Unit = {
    super.postStop
    log.debug("has shut down")
  }

  override def receive: PartialFunction[Any, Unit] = {
    case Close =>
      close()
      context stop self
    case q =>
      log.warning(s"received unknown message type: ${q.getClass}")
  }

  /**
    * This method takes a response which is a Try[Any] and sends it to the caller according to whether it is a success or failure.
    *
    * @param caller   the actor which requested the response.
    * @param response the response wrapped in Try.
    */
  def sendReply(caller: ActorRef, response: Try[Any]): Unit =
    caller ! (response match {
      case Success(x) => x
      case Failure(x) => Status.Failure(x)
    })

  def maybeLog(w: String, z: => Any): Unit = if (log.isDebugEnabled) log.debug(w, z)

  // TODO resolve duplicate code fragment
  def getTimeout(t: String): Timeout = {
    val durationR = """(\d+)\s*(\w+)""".r
    val timeout = t match {
      case durationR(n, s) => new Timeout(FiniteDuration(n.toLong, s))
      case _ => Timeout(10 seconds)
    }
    log.debug(s"setting timeout to: $timeout")
    timeout
  }

  def close(): Unit = {
    // NOTE: close down any non-actor resources (actors get closed anyway).
  }
}

trait Responder[K, W] extends CleanerCollector[K, W] {

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
  def prepareResponse[Y: ClassTag](wKys: Seq[Try[(K, W)]]): Try[(Y, Seq[Throwable])] = {
    val (kWsm, xs) = cleanAndCollect(wKys)
    if (isStrict && xs.nonEmpty) Failure(xs.head)
    else kWsm match {
      case y: Y => Success(y -> xs)
      case _ => Failure(MapReduceException(s"${kWsm.getClass} did not match expected type: ${implicitly[ClassTag[Y]]}"))
    }
  }
}

trait CleanerCollector[K, W] {
  /**
    * Method to clean exceptions from the input, and collect the results together, returning appropriate output.
    *
    * NOTE: this should be implemented inside an Actor to help retain referential transparency.
    *
    * @param kWys the input of type Seq of Try of (K, W).
    * @return the output of type (Map[K, Seq of W], Seq of Throwable).
    */
  def cleanAndCollect(kWys: Seq[Try[(K, W)]]): (Map[K, Seq[W]], Seq[Throwable]) = {
    val kWsm = mutable.HashMap[K, Seq[W]]() // mutable
    val xs = mutable.ListBuffer[Throwable]() // mutable
    for (kWy <- kWys) {
      sequence(kWy) match {
        case Right((k, w)) => kWsm put(k, w +: kWsm.getOrElse(k, Nil))
        case Left(x) => x +=: xs
      }
    }
    (kWsm.toMap, xs.toSeq)
  }
}

case class MapReduceException(context: String, x: Throwable) extends Throwable(context, x)

object MapReduceException {
  def apply(context: String): MapReduceException = MapReduceException(context, null)
}

/**
  * CONSIDER Don't think we really need this close mechanism. Akka does everything for us.
  */
object Close