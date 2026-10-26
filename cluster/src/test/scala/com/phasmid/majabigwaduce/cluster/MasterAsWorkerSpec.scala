package com.phasmid.majabigwaduce.cluster

import com.phasmid.majabigwaduce.core.*
import com.phasmid.majabigwaduce.examples.countwords.{CountWords, ResourceHttpClient}
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

/** Two nodes which both run a worker -- including the node which runs the Master. */
class MasterAsWorkerSpec extends AnyWordSpec with should.Matchers with ScalaFutures with BeforeAndAfterAll {

  private var systems: Seq[ActorSystem[Nothing]] = Nil
  private def masterNode = systems.head
  private def workerNode = systems(1)

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = Span(20, Seconds))

  override def beforeAll(): Unit =
    systems = TestCluster.start(2, "majabigwaduce.cluster.min-workers = 2")
    systems.foreach(ClusterWorker.start)

  override def afterAll(): Unit = TestCluster.stop(systems)

  "ClusterWorkerPool" should {
    "spread the reducers evenly across both nodes, alternating between them" in {
      val (_, wf) = TestCluster.acquire(masterNode, FoldDef[String, Int, Int](_ + _, () => 0), 4)
      val reducers = wf.futureValue.reducers
      reducers should have size 4
      // NOTE refs to the master node's own reducers arrive without being serialized, so they have local scope.
      val onMaster = reducers.map(_.path.address.hasLocalScope)
      onMaster.count(identity) shouldBe 2
      reducers.filterNot(_.path.address.hasLocalScope).map(_.path.address).toSet shouldBe Set(TestCluster.address(workerNode))
      onMaster.sliding(2).forall { case Seq(a, b) => a != b; case _ => true } shouldBe true
    }
  }

  "CountWords" should {
    "count words with its reducers on both nodes" in {
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
