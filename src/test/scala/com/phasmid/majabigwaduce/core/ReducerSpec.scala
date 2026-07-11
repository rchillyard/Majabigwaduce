package com.phasmid.majabigwaduce.core

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpecLike

class ReducerSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with should.Matchers {

  "A Reducer" must {
    "reduce a non-empty sequence using the combining function" in {
      val g: (Int, Int) => Int = _ + _
      val reducer = spawn(Reducer[String, Int, Int](g))
      val probe = createTestProbe[ReduceResult[String, Int]]()
      reducer ! DoReduce[String, Int, Int](Intermediate("key", Seq(1, 2, 3, 4)), probe.ref)
      probe.expectMessage(ReduceResult("key", Right(10)))
    }

    "return a Left when reducing an empty sequence" in {
      val g: (Int, Int) => Int = _ + _
      val reducer = spawn(Reducer[String, Int, Int](g))
      val probe = createTestProbe[ReduceResult[String, Int]]()
      reducer ! DoReduce[String, Int, Int](Intermediate("key", Seq.empty[Int]), probe.ref)
      probe.receiveMessage() match
        case ReduceResult("key", Left(x: Throwable)) => x shouldBe a[UnsupportedOperationException]
        case other => fail(s"unexpected result: $other")
    }

    "return a Left when the combining function throws" in {
      val g: (Int, Int) => Int = (_, _) => throw new ArithmeticException("boom")
      val reducer = spawn(Reducer[String, Int, Int](g))
      val probe = createTestProbe[ReduceResult[String, Int]]()
      reducer ! DoReduce[String, Int, Int](Intermediate("key", Seq(1, 2)), probe.ref)
      probe.receiveMessage() match
        case ReduceResult("key", Left(x: Throwable)) => x shouldBe an[ArithmeticException]
        case other => fail(s"unexpected result: $other")
    }

    "stop upon receiving CloseReducer" in {
      val g: (Int, Int) => Int = _ + _
      val reducer = spawn(Reducer[String, Int, Int](g))
      reducer ! CloseReducer()
      createTestProbe().expectTerminated(reducer)
    }
  }

  "A Reducer_Fold" must {
    "fold a non-empty sequence using the combining function and zero value" in {
      val g: (Int, Int) => Int = _ + _
      val reducer = spawn(Reducer_Fold[String, Int, Int](g, 0))
      val probe = createTestProbe[ReduceResult[String, Int]]()
      reducer ! DoReduce[String, Int, Int](Intermediate("key", Seq(1, 2, 3)), probe.ref)
      probe.expectMessage(ReduceResult("key", Right(6)))
    }

    "fold an empty sequence to the zero value" in {
      val g: (Int, Int) => Int = _ + _
      val reducer = spawn(Reducer_Fold[String, Int, Int](g, 0))
      val probe = createTestProbe[ReduceResult[String, Int]]()
      reducer ! DoReduce[String, Int, Int](Intermediate("key", Seq.empty[Int]), probe.ref)
      probe.expectMessage(ReduceResult("key", Right(0)))
    }

    "return a Left when the combining function throws" in {
      val g: (Int, Int) => Int = (_, _) => throw new ArithmeticException("boom")
      val reducer = spawn(Reducer_Fold[String, Int, Int](g, 0))
      val probe = createTestProbe[ReduceResult[String, Int]]()
      reducer ! DoReduce[String, Int, Int](Intermediate("key", Seq(1)), probe.ref)
      probe.receiveMessage() match
        case ReduceResult("key", Left(x: Throwable)) => x shouldBe an[ArithmeticException]
        case other => fail(s"unexpected result: $other")
    }
  }

  "Intermediate" must {
    "render a helpful toString" in {
      Intermediate("key", Seq(1, 2, 3)).toString shouldBe "Intermediate: with k2=key and 3 elements"
    }
  }
}
