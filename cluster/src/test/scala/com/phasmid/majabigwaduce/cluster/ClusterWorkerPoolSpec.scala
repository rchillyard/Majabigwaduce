package com.phasmid.majabigwaduce.cluster

import com.phasmid.majabigwaduce.core.MapReduceException
import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpecLike

import scala.util.{Failure, Success}

class ClusterWorkerPoolSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with should.Matchers {

  "shares" should {
    "split reducers as evenly as possible, larger shares first" in {
      ClusterWorkerPool.shares(4, 2) shouldBe Seq(2, 2)
      ClusterWorkerPool.shares(5, 2) shouldBe Seq(3, 2)
      ClusterWorkerPool.shares(2, 3) shouldBe Seq(1, 1, 0)
    }
  }

  "interleave" should {
    "take one from each sequence in turn" in {
      ClusterWorkerPool.interleave(Seq(Seq(1, 3, 5), Seq(2, 4))) shouldBe Seq(1, 2, 3, 4, 5)
      ClusterWorkerPool.interleave(Seq.empty[Seq[Int]]) shouldBe Nil
    }
  }

  "requireJavaSerialization" should {
    "explain how to switch Java serialization on, when it is off" in {
      // NOTE the test kit's system uses the default configuration, in which Java serialization is off.
      ClusterWorkerPool.requireJavaSerialization(system) match
        case Failure(x) =>
          x shouldBe a[MapReduceException]
          x.getMessage should include("pekko.actor.allow-java-serialization = on")
        case Success(_) => fail("expected requireJavaSerialization to fail")
    }
  }
}
