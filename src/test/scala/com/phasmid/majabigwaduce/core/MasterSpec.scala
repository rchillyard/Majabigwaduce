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

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "A map-reduce" must {
    "return map" in {
      val _5seconds = FiniteDuration(5L, scala.concurrent.duration.SECONDS)
      implicit val timeout: Timeout = Timeout(_5seconds)
      implicit val executionContext: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global
      implicit val config: Config = ConfigFactory.load.getConfig("Matrix")
      implicit val actors: Actors = Actors(system, config)
      val mr: MapReduceFirst[String, String, String, String] = MapReduceFirst.create(v => (v, v), (v1, _) => v1)
      val rf: Future[Map[String, String]] = mr.apply(Seq("Hello", "Goodbye"))
      Await.ready(rf, _5seconds)
      whenReady(rf) {
        r => r shouldBe Map("Hello" -> "Hello", "Goodbye" -> "Goodbye")
      }
    }
    "return failure status from Mapper" in {
      val _5seconds = FiniteDuration(5L, scala.concurrent.duration.SECONDS)
      implicit val timeout: Timeout = Timeout(_5seconds)
      implicit val executionContext: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global
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
      implicit val timeout: Timeout = Timeout(_5seconds)
      implicit val executionContext: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global
      implicit val config: Config = ConfigFactory.load.getConfig("Matrix")
      implicit val actors: Actors = Actors(system, config)
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
  }
}
