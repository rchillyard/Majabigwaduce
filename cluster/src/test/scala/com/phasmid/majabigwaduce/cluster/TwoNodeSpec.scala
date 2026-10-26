package com.phasmid.majabigwaduce.cluster

import com.phasmid.majabigwaduce.core.*
import com.phasmid.majabigwaduce.examples.countwords.{CountWords, ResourceHttpClient}
import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.util.Timeout
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should
import org.scalatest.time.{Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

/** A master node, which runs no worker, and a worker node. */
class TwoNodeSpec extends AnyWordSpec with should.Matchers with ScalaFutures with BeforeAndAfterAll {

  private var systems: Seq[ActorSystem[Nothing]] = Nil
  private def masterNode = systems.head
  private def workerNode = systems(1)

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = Span(20, Seconds))

  override def beforeAll(): Unit =
    systems = TestCluster.start(2)
    ClusterWorker.start(workerNode)

  override def afterAll(): Unit = TestCluster.stop(systems)

  "WorkerPool.forSystem" should {
    "choose ClusterWorkerPool for a cluster member" in {
      WorkerPool.forSystem(masterNode) shouldBe ClusterWorkerPool
    }
  }

  "ClusterWorkerPool" should {
    "spawn the reducers on the worker node and keep the mappers local" in {
      val (_, wf) = TestCluster.acquire(masterNode, FoldDef[String, Int, Int](_ + _, () => 0), 3)
      val workers = wf.futureValue
      workers.reducers should have size 3
      workers.reducers.map(_.path.address).toSet shouldBe Set(TestCluster.address(workerNode))
      workers.mappers should have size 1
      workers.mappers.head.path.address.hasLocalScope shouldBe true
    }

    "stop a job's remote reducers when its master stops" in {
      val (master, wf) = TestCluster.acquire(masterNode, FoldDef[String, Int, Int](_ + _, () => 0), 2)
      val reducers = wf.futureValue.reducers
      val testKit = ActorTestKit(masterNode)
      val probe = testKit.createTestProbe[Any]()
      master ! ()
      reducers.foreach(r => probe.expectTerminated(r, 15.seconds))
    }

    "fail fast, with a helpful message, when the reducer function captures something non-serializable" in {
      class Handle(val n: Int)
      val h = Handle(1)
      val start = System.nanoTime()
      val (_, wf) = TestCluster.acquire(masterNode, FoldDef[String, Int, Int]((acc, w) => acc + w + h.n, () => 0), 2)
      val x = wf.failed.futureValue
      (System.nanoTime() - start).nanos should be < 5.seconds
      x shouldBe a[MapReduceException]
      x.getMessage should include("not serializable")
    }
  }

  "CountWords" should {
    "count words with its reducers on the worker node" in {
      given system: ActorSystem[Nothing] = masterNode
      given logger: Logger = LoggerFactory.getLogger(classOf[CountWords])
      given config: com.typesafe.config.Config = TestCluster.jobConfig
      given timeout: Timeout = Timeout(15.seconds)
      given ec: ExecutionContext = system.executionContext
      val cw = CountWords(new ResourceHttpClient("/countwords").getResource)
      try cw(Seq("https://www.bbc.com/doc1", "https://www.bbc.com/doc2", "https://www.cnn.com/doc3")).futureValue shouldBe 11
      finally cw.close()
    }
  }
}
