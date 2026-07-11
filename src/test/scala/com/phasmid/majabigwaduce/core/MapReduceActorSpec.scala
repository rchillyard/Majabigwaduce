package com.phasmid.majabigwaduce.core

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

// NOTE: classic Akka's MapReduceActor base class handled an "unknown message type" case (since
// receive: PartialFunction[Any, Unit] could be sent anything) and exposed a sendReply helper for
// wrapping a Try into a Status.Failure. Both are gone by construction in Typed -- a typed actor's
// mailbox can only ever hold values of its own declared command type (checked at compile time),
// and a reply can only ever be the declared reply type, so there is nothing left to test there.
// What remains of the shared behavior -- start/stop logging, and Close stopping the actor -- is
// tested here directly against MapReduceActor.withLifecycle using a minimal probe protocol.
class MapReduceActorSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with should.Matchers {

  private val logger = LoggerFactory.getLogger("MapReduceActorSpecProbe")

  sealed trait ProbeCommand
  case class Ping(replyTo: ActorRef[String]) extends ProbeCommand
  case object CloseProbe extends ProbeCommand

  private def probeBehavior: Behavior[ProbeCommand] =
    MapReduceActor.withLifecycle(logger) { _ =>
      {
        case Ping(replyTo) =>
          replyTo ! "pong"
          Behaviors.same
        case CloseProbe =>
          Behaviors.stopped
      }
    }

  "MapReduceActor.withLifecycle" must {
    "dispatch messages to the supplied handler" in {
      val ref = spawn(probeBehavior)
      val probe = createTestProbe[String]()
      ref ! Ping(probe.ref)
      probe.expectMessage("pong")
    }

    "stop the actor upon receiving its own Close command" in {
      val ref = spawn(probeBehavior)
      ref ! CloseProbe
      createTestProbe().expectTerminated(ref)
    }
  }
}
