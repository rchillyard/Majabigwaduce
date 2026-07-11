package com.phasmid.majabigwaduce.core

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.scaladsl.Behaviors
import akka.util.Timeout
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.{Level, Logger => LogbackLogger}
import ch.qos.logback.core.read.ListAppender
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.matchers.should
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters.*

class ActorsSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with should.Matchers {

  private val config: Config = ConfigFactory.parseString("exceptionStack = false")

  "Actors.createActor" must {
    "name the created actor after the given base name, plus a unique suffix" in {
      val actors = Actors(system, config)
      val ref = actors.createActor[String]((b, n) => system.systemActorOf(b, n), Some("foo"), Behaviors.ignore[String])
      ref.path.name should startWith("foo-")
    }

    "default to the base name \"Nemo\" when no name is given" in {
      val actors = Actors(system, config)
      val ref = actors.createActor[String]((b, n) => system.systemActorOf(b, n), None, Behaviors.ignore[String])
      ref.path.name should startWith("Nemo-")
    }

    // NOTE: the suffix is fixed per Actors instance (computed once at construction), not per
    // createActor call -- so uniqueness across a shared base name only holds across distinct
    // Actors instances. (Within one Actors instance, callers must vary the base name themselves,
    // as Master does by appending "-$i" for its reducers.)
    "generate distinct names for actors created from different Actors instances sharing the same base name" in {
      val ref1 = Actors(system, config).createActor[String]((b, n) => system.systemActorOf(b, n), Some("dup"), Behaviors.ignore[String])
      val ref2 = Actors(system, config).createActor[String]((b, n) => system.systemActorOf(b, n), Some("dup"), Behaviors.ignore[String])
      ref1.path.name should not be ref2.path.name
    }
  }

  "Actors.logException" must {
    // NOTE: Actors.logException now logs via a plain SLF4J logger (not through the actor system's
    // own logging adapter), so we capture it with a Logback ListAppender rather than
    // akka's LoggingTestKit (which only observes log events published via the actor system).
    def withCapturedLogs(test: ListAppender[ILoggingEvent] => Unit): Unit =
      val logbackLogger = LoggerFactory.getLogger(classOf[Actors]).asInstanceOf[LogbackLogger]
      val appender = new ListAppender[ILoggingEvent]()
      appender.start()
      logbackLogger.addAppender(appender)
      try test(appender)
      finally logbackLogger.detachAppender(appender)

    "log at error level, with the full exception, when exceptionStack is true" in withCapturedLogs { appender =>
      val actors = Actors(system, ConfigFactory.parseString("exceptionStack = true"))
      actors.logException("boom", new RuntimeException("oops"))
      val events = appender.list.asScala
      events.exists(e => e.getLevel == Level.ERROR && e.getFormattedMessage == "boom") shouldBe true
    }

    "log at warning level, with just the localized message, when exceptionStack is false" in withCapturedLogs { appender =>
      val actors = Actors(system, ConfigFactory.parseString("exceptionStack = false"))
      actors.logException("boom", new RuntimeException("oops"))
      val events = appender.list.asScala
      events.exists(e => e.getLevel == Level.WARN && e.getFormattedMessage == "boom: oops") shouldBe true
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

  "Actors.getTimeout" must {
    "parse a well-formed timeout string" in {
      Actors.getTimeout("5 seconds") shouldBe Timeout(5.seconds)
    }

    "fall back to a default 10 second timeout for a malformed string" in {
      Actors.getTimeout("garbage") shouldBe Timeout(10.seconds)
    }
  }

}
