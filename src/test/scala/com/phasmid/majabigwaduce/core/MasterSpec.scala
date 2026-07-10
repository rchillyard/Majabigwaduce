package com.phasmid.majabigwaduce.core

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should
import org.scalatest.{BeforeAndAfterAll, wordspec}

import scala.concurrent.{Await, Future}
//import org.scalatest.time.{Seconds, Span}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

class MasterSpec
  extends TestKit(ActorSystem("MySpec"))
    with ImplicitSender
    with wordspec.AnyWordSpecLike
    with should.Matchers
    with ScalaFutures
    with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

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
    "return failure status from Mapper" in {
      val _5seconds = FiniteDuration(5L, scala.concurrent.duration.SECONDS)

      given timeout: Timeout = Timeout(_5seconds)

      given executionContext: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

      val f: (String, String) => Try[(Int, String)] = (_, _) => Failure(MapReduceException("test"))
      val mapper = system.actorOf(Props.create(classOf[Mapper[String, String, Int, String]], f))
      val rf = mapper ask KeyValuePairs(Seq("hello" -> "Fred", "goodbye" -> "Thursday"))
      Await.ready(rf, _5seconds)
      whenReady(rf.failed) {
        x => x shouldBe a[MapReduceException]
      }
    }
  }

  "A master" must {
    "return map" in {
      val _5seconds = FiniteDuration(5L, scala.concurrent.duration.SECONDS)

      given timeout: Timeout = Timeout(_5seconds)

      given executionContext: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

      given config: Config = ConfigFactory.load.getConfig("majabigwaduce.Matrix")

      given actors: Actors = Actors(system, config)

      val f: (String, String) => Try[(String, String)] = (k, v) => Success((k, v))
      val g: (String, String) => Try[String] = (v1, _) => Success(v1)
      val props = Props.create(classOf[Master[String, String, String, String, String]], config, f, g)
      val master: ActorRef = actors.createActor(system, Some("master"), props)
      val rf: Future[Response[String, String]] = (master ? Seq("Hello" -> "X", "Goodbye" -> "Y")).mapTo[Response[String, String]]
      Await.ready(rf, _5seconds)
      whenReady(rf) {
        r => r shouldBe Response(Map(), Map("Hello" -> "X", "Goodbye" -> "Y"))
      }
    }
    "aggregate a reducer failure into the left side of Response while still returning successful reductions on the right" in {
      val _5seconds = FiniteDuration(5L, scala.concurrent.duration.SECONDS)

      given timeout: Timeout = Timeout(_5seconds)

      given executionContext: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

      given config: Config = ConfigFactory.load.getConfig("majabigwaduce.Matrix")

      given actors: Actors = Actors(system, config)

      val f: (String, Int) => Try[(String, Int)] = (k, v) => Success((k, v))
      // NOTE: throws whenever either operand is 99, so that the "a" group (which contains 99) fails to reduce.
      val g: (Int, Int) => Int = (acc, w) => if (acc == 99 || w == 99) throw new ArithmeticException("boom") else acc + w
      val props = Props.create(classOf[Master[String, Int, String, Int, Int]], config, f, g)
      val master: ActorRef = actors.createActor(system, Some("master-reducer-failure"), props)
      val rf: Future[Response[String, Int]] = (master ? Seq("a" -> 1, "a" -> 99, "b" -> 3, "b" -> 4)).mapTo[Response[String, Int]]
      Await.ready(rf, _5seconds)
      whenReady(rf) {
        r =>
          r.right shouldBe Map("b" -> 7)
          r.left.keySet shouldBe Set("a")
          r.left("a") shouldBe an[ArithmeticException]
      }
    }
  }

  "A Master_Fold" must {
    "fold values by key using the zero value and the combining function" in {
      val _5seconds = FiniteDuration(5L, scala.concurrent.duration.SECONDS)

      given timeout: Timeout = Timeout(_5seconds)

      given executionContext: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

      given config: Config = ConfigFactory.load.getConfig("majabigwaduce.Matrix")

      given actors: Actors = Actors(system, config)

      val f: (String, Int) => Try[(String, Int)] = (k, v) => Success((k, v))
      val g: (Int, Int) => Int = _ + _
      val props = Props.create(classOf[Master_Fold[String, Int, String, Int, Int]], config, f, g, () => 0)
      val master: ActorRef = actors.createActor(system, Some("master-fold"), props)
      val rf: Future[Response[String, Int]] = (master ? Seq("a" -> 1, "a" -> 2, "b" -> 3, "b" -> 4)).mapTo[Response[String, Int]]
      Await.ready(rf, _5seconds)
      whenReady(rf) {
        r => r shouldBe Response(Map(), Map("a" -> 3, "b" -> 7))
      }
    }
  }

  "A Master_First" must {
    "return map from a Seq[V1] message with no grouping key" in {
      val _5seconds = FiniteDuration(5L, scala.concurrent.duration.SECONDS)

      given timeout: Timeout = Timeout(_5seconds)

      given executionContext: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

      given config: Config = ConfigFactory.load.getConfig("majabigwaduce.Matrix")

      given actors: Actors = Actors(system, config)

      val f: String => Try[(String, String)] = v => Success((v, v))
      val g: (String, String) => String = (v1, _) => v1
      val props = Props.create(classOf[Master_First[String, String, String, String]], config, f, g)
      val master: ActorRef = actors.createActor(system, Some("master-first"), props)
      val rf: Future[Response[String, String]] = (master ? Seq("Hello", "Goodbye")).mapTo[Response[String, String]]
      Await.ready(rf, _5seconds)
      whenReady(rf) {
        r => r shouldBe Response(Map(), Map("Hello" -> "Hello", "Goodbye" -> "Goodbye"))
      }
    }
  }

  "A Master_First_Fold" must {
    "fold values grouped by a derived key using the zero value and the combining function" in {
      val _5seconds = FiniteDuration(5L, scala.concurrent.duration.SECONDS)

      given timeout: Timeout = Timeout(_5seconds)

      given executionContext: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

      given config: Config = ConfigFactory.load.getConfig("majabigwaduce.Matrix")

      given actors: Actors = Actors(system, config)

      // NOTE: groups words by length, folding a count of 1 per word.
      val f: String => Try[(Int, Int)] = s => Success((s.length, 1))
      val g: (Int, Int) => Int = _ + _
      val props = Props.create(classOf[Master_First_Fold[String, Int, Int, Int]], config, f, g, () => 0)
      val master: ActorRef = actors.createActor(system, Some("master-first-fold"), props)
      val rf: Future[Response[Int, Int]] = (master ? Seq("a", "bb", "ccc", "dd")).mapTo[Response[Int, Int]]
      Await.ready(rf, _5seconds)
      whenReady(rf) {
        r => r shouldBe Response(Map(), Map(1 -> 1, 2 -> 2, 3 -> 1))
      }
    }
  }
}
