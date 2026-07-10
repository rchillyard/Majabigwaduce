package com.phasmid.majabigwaduce.core

import akka.actor.{ActorSystem, Props}
import akka.testkit.{EventFilter, ImplicitSender, TestActorRef, TestKit}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should
import org.scalatest.{BeforeAndAfterAll, wordspec}

import scala.concurrent.duration.*
import scala.util.{Failure, Success}

// NOTE: MapReduceActor is abstract only in name -- every member required by Actor is already
// implemented, so a bare subclass is enough to exercise its behavior directly.
class ProbeActor extends MapReduceActor

class MapReduceActorSpec
  extends TestKit(ActorSystem("MapReduceActorSpec",
    ConfigFactory.parseString("""akka.loggers = ["akka.testkit.TestEventListener"]""").withFallback(ConfigFactory.load())))
    with ImplicitSender
    with wordspec.AnyWordSpecLike
    with should.Matchers
    with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "A MapReduceActor" must {
    "log a warning and otherwise ignore a message of an unrecognized type" in {
      val actor = system.actorOf(Props(new ProbeActor))
      EventFilter.warning(start = "received unknown message type", occurrences = 1).intercept {
        actor ! 42
      }
    }

    "invoke close() and stop itself upon receiving Close" in {
      val actor = system.actorOf(Props(new ProbeActor))
      watch(actor)
      actor ! Close
      expectTerminated(actor)
    }

    "send a Success value straight back to the caller via sendReply" in {
      val ref = TestActorRef(new ProbeActor)
      ref.underlyingActor.sendReply(testActor, Success("ok"))
      expectMsg("ok")
    }

    "wrap a Failure in akka.actor.Status.Failure via sendReply" in {
      val ref = TestActorRef(new ProbeActor)
      val x = new RuntimeException("boom")
      ref.underlyingActor.sendReply(testActor, Failure(x))
      expectMsg(akka.actor.Status.Failure(x))
    }

    "parse a well-formed timeout string" in {
      val ref = TestActorRef(new ProbeActor)
      ref.underlyingActor.getTimeout("5 seconds") shouldBe Timeout(5.seconds)
    }

    "fall back to a default 10 second timeout for a malformed string" in {
      val ref = TestActorRef(new ProbeActor)
      ref.underlyingActor.getTimeout("garbage") shouldBe Timeout(10.seconds)
    }
  }
}
