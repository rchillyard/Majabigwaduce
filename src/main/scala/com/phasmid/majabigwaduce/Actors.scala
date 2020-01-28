package com.phasmid.majabigwaduce

import akka.actor.{ActorRef, ActorSystem, Props}
import com.typesafe.config.Config

case class Actors(system: ActorSystem, config: Config) extends AutoCloseable {

  def createActor(maybeName: Option[String], props: Props): ActorRef = {
    val actorName = maybeName match {
      case Some(name) => name
      case None => "Nemo"
    }
    val actorId = s"$actorName-$suffix"
    system.log.debug(s"""createActor: $actorId of ${props.args.head}""")
    system.actorOf(props, actorId)
  }

  private val suffix = (System.currentTimeMillis.hashCode + Actors.getCount).toHexString

  def logException(m: => String, x: Throwable): Unit = if (exceptionStack) system.log.error(x, m) else system.log.warning(s"$m: ${x.getLocalizedMessage}")

  private lazy val exceptionStack = config.getBoolean("exceptionStack")

  // NOTE: mutable list of actor refs
  //  var actors = Seq[ActorRef]()

  def close(): Unit = {
    //    actors foreach(system.)
  }
}

object Actors {
  var count: Int = 0
  def getCount: Int = {count+=1; count}
}