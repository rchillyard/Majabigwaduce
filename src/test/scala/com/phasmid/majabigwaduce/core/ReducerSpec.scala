package com.phasmid.majabigwaduce.core

import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should
import org.scalatest.{BeforeAndAfterAll, wordspec}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext}

class ReducerSpec
  extends TestKit(ActorSystem("ReducerSpec"))
    with ImplicitSender
    with wordspec.AnyWordSpecLike
    with should.Matchers
    with ScalaFutures
    with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  private val _5seconds = FiniteDuration(5L, scala.concurrent.duration.SECONDS)

  given timeout: Timeout = Timeout(_5seconds)

  given executionContext: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

  "A Reducer" must {
    "reduce a non-empty sequence using the combining function" in {
      val g: (Int, Int) => Int = _ + _
      val reducer = system.actorOf(Props.create(classOf[Reducer[String, Int, Int]], g))
      reducer ! Intermediate("key", Seq(1, 2, 3, 4))
      expectMsg(("key", Right(10)))
    }

    "return a Left when reducing an empty sequence" in {
      val g: (Int, Int) => Int = _ + _
      val reducer = system.actorOf(Props.create(classOf[Reducer[String, Int, Int]], g))
      val rf = reducer ask Intermediate("key", Seq.empty[Int])
      Await.ready(rf, _5seconds)
      whenReady(rf) {
        case (k: String, Left(x: Throwable)) =>
          k shouldBe "key"
          x shouldBe a[UnsupportedOperationException]
        case other => fail(s"unexpected result: $other")
      }
    }

    "return a Left when the combining function throws" in {
      val g: (Int, Int) => Int = (_, _) => throw new ArithmeticException("boom")
      val reducer = system.actorOf(Props.create(classOf[Reducer[String, Int, Int]], g))
      val rf = reducer ask Intermediate("key", Seq(1, 2))
      Await.ready(rf, _5seconds)
      whenReady(rf) {
        case (k: String, Left(x: Throwable)) =>
          k shouldBe "key"
          x shouldBe an[ArithmeticException]
        case other => fail(s"unexpected result: $other")
      }
    }
  }

  "A Reducer_Fold" must {
    "fold a non-empty sequence using the combining function and zero value" in {
      val g: (Int, Int) => Int = _ + _
      val reducer = system.actorOf(Props.create(classOf[Reducer_Fold[String, Int, Int]], g, () => 0))
      reducer ! Intermediate("key", Seq(1, 2, 3))
      expectMsg(("key", Right(6)))
    }

    "fold an empty sequence to the zero value" in {
      val g: (Int, Int) => Int = _ + _
      val reducer = system.actorOf(Props.create(classOf[Reducer_Fold[String, Int, Int]], g, () => 0))
      reducer ! Intermediate("key", Seq.empty[Int])
      expectMsg(("key", Right(0)))
    }

    "return a Left when the combining function throws" in {
      val g: (Int, Int) => Int = (_, _) => throw new ArithmeticException("boom")
      val reducer = system.actorOf(Props.create(classOf[Reducer_Fold[String, Int, Int]], g, () => 0))
      val rf = reducer ask Intermediate("key", Seq(1))
      Await.ready(rf, _5seconds)
      whenReady(rf) {
        case (k: String, Left(x: Throwable)) =>
          k shouldBe "key"
          x shouldBe an[ArithmeticException]
        case other => fail(s"unexpected result: $other")
      }
    }
  }

  "Intermediate" must {
    "render a helpful toString" in {
      Intermediate("key", Seq(1, 2, 3)).toString shouldBe "Intermediate: with k2=key and 3 elements"
    }
  }
}
