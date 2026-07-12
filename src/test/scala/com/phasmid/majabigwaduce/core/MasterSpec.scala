package com.phasmid.majabigwaduce.core

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

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

  "A master with multiple mappers" must {
    "merge mapper responses correctly when the same K2 key lands in different mapper chunks" in {
      val config: Config = ConfigFactory.load.getConfig("majabigwaduce.Matrix")
        .withValue("mappers", ConfigValueFactory.fromAnyRef(2))
      val actors = Actors(system, config)

      val f: (String, Int) => Try[(String, Int)] = (k, v) => Success((k, v))
      val g: (Int, Int) => Int = _ + _
      val master = actors.createActor[MasterCommand[String, Int, String, Int]]((b, n) => system.systemActorOf(b, n), Some("master-multi-mapper"), Master(config, f, g))
      val probe = createTestProbe[Try[Response[String, Int]]]()
      // 4 elements, mappers=2 -> chunk 1 = [k1->1, k2->2], chunk 2 = [k1->3, k2->4]: both "k1"
      // and "k2" land in both chunks, exercising the deep-merge in Master.mergeMapperResponses.
      master ! ComputeSeq(Seq("k1" -> 1, "k2" -> 2, "k1" -> 3, "k2" -> 4), probe.ref)
      probe.expectMessage(Success(Response(Map(), Map("k1" -> 4, "k2" -> 6))))
    }

    "combine successful results from every mapper chunk even when other chunks fail (forgiving mode)" in {
      val config: Config = ConfigFactory.load.getConfig("majabigwaduce.Matrix")
        .withValue("mappers", ConfigValueFactory.fromAnyRef(2))
        .withValue("forgiving", ConfigValueFactory.fromAnyRef(true))
      val actors = Actors(system, config)

      val f: (String, Int) => Try[(String, Int)] = (k, v) => if v < 0 then Failure(MapReduceException(s"bad value for $k")) else Success((k, v))
      val g: (Int, Int) => Int = _ + _
      val master = actors.createActor[MasterCommand[String, Int, String, Int]]((b, n) => system.systemActorOf(b, n), Some("master-multi-mapper-forgiving"), Master(config, f, g))
      val probe = createTestProbe[Try[Response[String, Int]]]()
      // chunk 1 = [a->1, bad1->-1], chunk 2 = [b->2, bad2->-1]: each chunk has one failing
      // element, so this only succeeds if the merge doesn't drop either chunk's good result.
      master ! ComputeSeq(Seq("a" -> 1, "bad1" -> -1, "b" -> 2, "bad2" -> -1), probe.ref)
      probe.expectMessage(Success(Response(Map(), Map("a" -> 1, "b" -> 2))))
    }
  }
}

class MasterSplitAndMergeSpec extends org.scalatest.wordspec.AnyWordSpec with should.Matchers {

  "Master.splitIntoChunks" must {
    "split a sequence into at most n contiguous chunks" in {
      Master.splitIntoChunks(Seq(1, 2, 3, 4, 5), 2) shouldBe Seq(Seq(1, 2, 3), Seq(4, 5))
    }
    "never produce more chunks than n" in {
      val chunks = Master.splitIntoChunks(Seq(1, 2, 3), 10)
      chunks.length should be <= 10
      chunks shouldBe Seq(Seq(1), Seq(2), Seq(3))
    }
    "produce a single chunk when n <= 1" in {
      Master.splitIntoChunks(Seq(1, 2, 3), 1) shouldBe Seq(Seq(1, 2, 3))
      Master.splitIntoChunks(Seq(1, 2, 3), 0) shouldBe Seq(Seq(1, 2, 3))
    }
    "produce no chunks for an empty input" in {
      Master.splitIntoChunks(Seq.empty[Int], 4) shouldBe Seq.empty
    }
  }

  "Master.mergeMapperResponses" must {
    "deep-merge same-key results across chunks and concatenate exceptions" in {
      val x1 = new RuntimeException("boom1")
      val x2 = new RuntimeException("boom2")
      val r1 = MapperResponse(Map("k1" -> Seq(1), "k2" -> Seq(2)), Seq(x1))
      val r2 = MapperResponse(Map("k1" -> Seq(3), "k2" -> Seq(4)), Seq(x2))
      val merged = Master.mergeMapperResponses(Seq(r1, r2))
      merged.result("k1") should contain theSameElementsAs Seq(1, 3)
      merged.result("k2") should contain theSameElementsAs Seq(2, 4)
      merged.exceptions shouldBe Seq(x1, x2)
    }
  }
}
