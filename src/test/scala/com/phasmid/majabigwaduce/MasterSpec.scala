package com.phasmid.majabigwaduce

import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should
import org.scalatest.{BeforeAndAfterAll, wordspec}

import scala.concurrent.Await
//import org.scalatest.time.{Seconds, Span}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Try}

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


  "A master" must {
    "return map and empty throwable list" in {
      //      val mapper = system.actorOf(  def createProps: Props = Props(new Master_First(config, f, g)))
      //      mapper ! KeyValueSeq(Seq("hello"->"Fred", "goodbye"->"Thursday"))
      //      expectMsg( (Map(207022353 -> List("THURSDAY"), 99162322 -> List("FRED"))) -> List())
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

}
