package com.phasmid.majabigwaduce

import akka.actor.{Actor, ActorLogging}
import akka.util.Timeout

import scala.concurrent.duration._
import scala.language.postfixOps

abstract class MapReduceActor extends Actor with ActorLogging {
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
      log.warning(s"received unknown message type: $q")
  }

  def maybeLog(w: String, z: => Any): Unit = if (log.isDebugEnabled) log.debug(w, z)

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
    // close down any non-actor resources (actors get closed anyway).
  }
}

case class MapReduceException(context: String, f: Throwable) extends Throwable(context, f)

/**
  * TODO Don't think we really need this close mechanism.
  * Akka does everything for us.
  *
  */
object Close