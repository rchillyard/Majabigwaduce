package com.phasmid.majabigwaduce.core

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Success, Try}

class MasterSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with should.Matchers with ScalaFutures {

  "A map-reduce" must {
    "return map" in {
      val _5seconds = FiniteDuration(5L, scala.concurrent.duration.SECONDS)

      given timeout: Timeout = Timeout(_5seconds)

      given executionContext: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

      given config: Config = ConfigFactory.load.getConfig("majabigwaduce.Matrix")

      given actors: Actors = Actors(system, config)

      val mr: MapReduceFirst[String, String, String, String] = MapReduceFirst.create(v => (v, v), (v1, _) => v1)
      val rf: Future[Map[String, String]] = mr.apply(Seq("Hello", "Goodbye"))
      Await.ready(rf, _5seconds)
      whenReady(rf) {
        r => r shouldBe Map("Hello" -> "Hello", "Goodbye" -> "Goodbye")
      }
    }
  }

  "A master" must {
    "return map" in {
      val config: Config = ConfigFactory.load.getConfig("majabigwaduce.Matrix")
      val actors = Actors(system, config)

      val f: (String, String) => Try[(String, String)] = (k, v) => Success((k, v))
      val g: (String, String) => String = (v1, _) => v1
      val master = actors.createActor[MasterCommand[String, String, String, String]]((b, n) => system.systemActorOf(b, n), Some("master"), Master(config, f, g))
      val probe = createTestProbe[Try[Response[String, String]]]()
      master ! ComputeSeq(Seq("Hello" -> "X", "Goodbye" -> "Y"), probe.ref)
      probe.expectMessage(Success(Response(Map(), Map("Hello" -> "X", "Goodbye" -> "Y"))))
    }

    "aggregate a reducer failure into the left side of Response while still returning successful reductions on the right" in {
      val config: Config = ConfigFactory.load.getConfig("majabigwaduce.Matrix")
      val actors = Actors(system, config)

      val f: (String, Int) => Try[(String, Int)] = (k, v) => Success((k, v))
      // NOTE: throws whenever either operand is 99, so that the "a" group (which contains 99) fails to reduce.
      val g: (Int, Int) => Int = (acc, w) => if (acc == 99 || w == 99) throw new ArithmeticException("boom") else acc + w
      val master = actors.createActor[MasterCommand[String, Int, String, Int]]((b, n) => system.systemActorOf(b, n), Some("master-reducer-failure"), Master(config, f, g))
      val probe = createTestProbe[Try[Response[String, Int]]]()
      master ! ComputeSeq(Seq("a" -> 1, "a" -> 99, "b" -> 3, "b" -> 4), probe.ref)
      probe.receiveMessage() match
        case Success(r) =>
          r.right shouldBe Map("b" -> 7)
          r.left.keySet shouldBe Set("a")
          r.left("a") shouldBe an[ArithmeticException]
        case other => fail(s"unexpected result: $other")
    }
  }

  "A Master_Fold" must {
    "fold values by key using the zero value and the combining function" in {
      val config: Config = ConfigFactory.load.getConfig("majabigwaduce.Matrix")
      val actors = Actors(system, config)

      val f: (String, Int) => Try[(String, Int)] = (k, v) => Success((k, v))
      val g: (Int, Int) => Int = _ + _
      val master = actors.createActor[MasterCommand[String, Int, String, Int]]((b, n) => system.systemActorOf(b, n), Some("master-fold"), Master_Fold(config, f, g, () => 0))
      val probe = createTestProbe[Try[Response[String, Int]]]()
      master ! ComputeSeq(Seq("a" -> 1, "a" -> 2, "b" -> 3, "b" -> 4), probe.ref)
      probe.expectMessage(Success(Response(Map(), Map("a" -> 3, "b" -> 7))))
    }
  }

  "A Master_First" must {
    "return map from a Seq[V1] message with no grouping key" in {
      val config: Config = ConfigFactory.load.getConfig("majabigwaduce.Matrix")
      val actors = Actors(system, config)

      val f: String => Try[(String, String)] = v => Success((v, v))
      val g: (String, String) => String = (v1, _) => v1
      val master = actors.createActor[MasterCommand[Unit, String, String, String]]((b, n) => system.systemActorOf(b, n), Some("master-first"), Master_First(config, f, g))
      val probe = createTestProbe[Try[Response[String, String]]]()
      master ! ComputeSeq(Seq(() -> "Hello", () -> "Goodbye"), probe.ref)
      probe.expectMessage(Success(Response(Map(), Map("Hello" -> "Hello", "Goodbye" -> "Goodbye"))))
    }
  }

  "A Master_First_Fold" must {
    "fold values grouped by a derived key using the zero value and the combining function" in {
      val config: Config = ConfigFactory.load.getConfig("majabigwaduce.Matrix")
      val actors = Actors(system, config)

      // NOTE: groups words by length, folding a count of 1 per word.
      val f: String => Try[(Int, Int)] = s => Success((s.length, 1))
      val g: (Int, Int) => Int = _ + _
      val master = actors.createActor[MasterCommand[Unit, String, Int, Int]]((b, n) => system.systemActorOf(b, n), Some("master-first-fold"), Master_First_Fold(config, f, g, () => 0))
      val probe = createTestProbe[Try[Response[Int, Int]]]()
      master ! ComputeSeq(Seq("a", "bb", "ccc", "dd").map(() -> _), probe.ref)
      probe.expectMessage(Success(Response(Map(), Map(1 -> 1, 2 -> 2, 3 -> 1))))
    }
  }
}
