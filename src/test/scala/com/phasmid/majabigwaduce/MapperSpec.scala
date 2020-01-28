package com.phasmid.majabigwaduce

import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestActors, TestKit}
import akka.util.Timeout
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should
import org.scalatest.{BeforeAndAfterAll, wordspec}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Try}

class MapperSpec
  extends TestKit(ActorSystem("MySpec"))
    with ImplicitSender
    with wordspec.AnyWordSpecLike
    with should.Matchers
    with ScalaFutures
    with BeforeAndAfterAll {

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }


  "A mapper" must {
    "return map and empty throwable list" in {
      val f: (String, String) => Try[(Int, String)] = (k, v) => Try(k.hashCode, v.toUpperCase)
      val mapper = system.actorOf(Props.create(classOf[Mapper[String, String, Int, String]], f))
      mapper ! KeyValueSeq(Seq("hello" -> "Fred", "goodbye" -> "Thursday"))
      expectMsg(Map(207022353 -> List("THURSDAY"), 99162322 -> List("FRED")) -> List())
    }
    "return failure status" in {
      val _5seconds = FiniteDuration(5L, scala.concurrent.duration.SECONDS)
      implicit val timeout: Timeout = Timeout(_5seconds)
      implicit val executionContext: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global
      val f: (String, String) => Try[(Int, String)] = (_, _) => Failure(MapReduceException("test"))
      val mapper = system.actorOf(Props.create(classOf[Mapper[String, String, Int, String]], f))
      val rf = mapper ask KeyValueSeq(Seq("hello" -> "Fred", "goodbye" -> "Thursday"))
      Await.ready(rf, _5seconds)
      whenReady(rf.failed) {
        x => x shouldBe a[MapReduceException]
      }
    }
  }

  "A forgiving mapper" must {
    "return map and empty throwable list" in {
      val f: (String, String) => Try[(Int, String)] = (k, v) => Try(k.hashCode, v.toUpperCase)
      val mapper = system.actorOf(Props.create(classOf[Mapper_Forgiving[String, String, Int, String]], f))
      mapper ! KeyValueSeq(Seq("hello" -> "Fred", "goodbye" -> "Thursday"))
      expectMsg(Map(207022353 -> List("THURSDAY"), 99162322 -> List("FRED")) -> List())
    }
    "return failure status" in {
      val _5seconds = FiniteDuration(5L, scala.concurrent.duration.SECONDS)
      implicit val timeout: Timeout = Timeout(_5seconds)
      implicit val executionContext: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global
      val f: (String, String) => Try[(Int, String)] = (_, _) => Failure(MapReduceException("test"))
      val mapper = system.actorOf(Props.create(classOf[Mapper_Forgiving[String, String, Int, String]], f))
      val rf: Future[(Map[Int, List[String]], List[String])] = (mapper ask KeyValueSeq(Seq("hello" -> "Fred", "goodbye" -> "Thursday"))).mapTo[(Map[Int, List[String]], List[String])]
      Await.ready(rf, _5seconds)
      whenReady(rf) {
        case (m, xs) =>
          m.size shouldBe 0
          xs.size shouldBe 2
      }
    }
  }

  "An Echo actor" must {

    "send back messages unchanged" in {
      val echo = system.actorOf(TestActors.echoActorProps)
      echo ! "hello world"
      expectMsg("hello world")
    }

  }
}
