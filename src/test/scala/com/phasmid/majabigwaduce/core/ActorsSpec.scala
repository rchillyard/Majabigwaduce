package com.phasmid.majabigwaduce.core

import akka.actor.{ActorSystem, Props}
import akka.testkit.{EventFilter, ImplicitSender, TestKit}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.matchers.should
import org.scalatest.{BeforeAndAfterAll, wordspec}

class ActorsSpec
  extends TestKit(ActorSystem("ActorsSpec",
    ConfigFactory.parseString("""akka.loggers = ["akka.testkit.TestEventListener"]""").withFallback(ConfigFactory.load())))
    with ImplicitSender
    with wordspec.AnyWordSpecLike
    with should.Matchers
    with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  private val config: Config = ConfigFactory.parseString("exceptionStack = false")

  "Actors.createActor" must {
    "name the created actor after the given base name, plus a unique suffix" in {
      val actors = Actors(system, config)
      val ref = actors.createActor(system, Some("foo"), Props(new ProbeActor))
      ref.path.name should startWith("foo-")
    }

    "default to the base name \"Nemo\" when no name is given" in {
      val actors = Actors(system, config)
      val ref = actors.createActor(system, None, Props(new ProbeActor))
      ref.path.name should startWith("Nemo-")
    }

    // NOTE: the suffix is fixed per Actors instance (computed once at construction), not per
    // createActor call -- so uniqueness across a shared base name only holds across distinct
    // Actors instances. (Within one Actors instance, callers must vary the base name themselves,
    // as Master does by appending "-$i" for its reducers.)
    "generate distinct names for actors created from different Actors instances sharing the same base name" in {
      val ref1 = Actors(system, config).createActor(system, Some("dup"), Props(new ProbeActor))
      val ref2 = Actors(system, config).createActor(system, Some("dup"), Props(new ProbeActor))
      ref1.path.name should not be ref2.path.name
    }
  }

  "Actors.logException" must {
    "log at error level, with the full exception, when exceptionStack is true" in {
      val actors = Actors(system, ConfigFactory.parseString("exceptionStack = true"))
      EventFilter.error(message = "boom", occurrences = 1).intercept {
        actors.logException("boom", new RuntimeException("oops"))
      }
    }

    "log at warning level, with just the localized message, when exceptionStack is false" in {
      val actors = Actors(system, ConfigFactory.parseString("exceptionStack = false"))
      EventFilter.warning(message = "boom: oops", occurrences = 1).intercept {
        actors.logException("boom", new RuntimeException("oops"))
      }
    }
  }

  "Actors.close" must {
    "be a no-op" in {
      val actors = Actors(system, config)
      noException should be thrownBy actors.close()
    }
  }

  "Actors.getCount" must {
    "return monotonically increasing values" in {
      val a = Actors.getCount
      val b = Actors.getCount
      b should be > a
    }
  }
}
