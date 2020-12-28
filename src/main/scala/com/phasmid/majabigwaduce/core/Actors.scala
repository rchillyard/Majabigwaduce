package com.phasmid.majabigwaduce.core

import akka.actor.{ActorRef, ActorRefFactory, ActorSystem, Props}
import com.typesafe.config.Config

/**
  * Case class to deal with the construction and configuration of actors.
  *
  * CONSIDER eliminating this class and calling actorOf directly from context or system. See Issue #13.
  *
  * @param system the actor system.
  * @param config the configuration for this set of actors.
  */
case class Actors(system: ActorSystem, config: Config) extends AutoCloseable {

  /**
    * Create a new actor, using the appropriate factory (based on either system or context).
    *
    * @param factory   the appropriate actor ref factory.
    * @param maybeName an optional name.
    * @param props     the appropriate Props.
    * @return an ActorRef.
    */
  def createActor(factory: ActorRefFactory, maybeName: Option[String], props: Props): ActorRef = {
    val actorName = maybeName match {
      case Some(name) => name
      case None => "Nemo"
    }
    // CONSIDER eliminating this suffix now that we create actors hierarchically (i.e. we create them from context, except the master).
    val actorId = s"$actorName-$suffix"
    system.log.debug(s"""createActor: $actorId of ${props.args.headOption.getOrElse(().getClass)}""")
    factory.actorOf(props, actorId)
  }

  private val suffix = (System.nanoTime().hashCode + Actors.getCount).toHexString

  // TEST
  def logException(m: => String, x: Throwable = null): Unit = if (exceptionStack) system.log.error(x, m) else system.log.warning(s"$m: ${x.getLocalizedMessage}")

  // TEST
  private lazy val exceptionStack = config.getBoolean("exceptionStack")

  def close(): Unit = {}
}

object Actors {
  // NOTE: consciously using var here.
  var count: Int = 0

  def getCount: Int = {
    count += 1
    count
  }
}