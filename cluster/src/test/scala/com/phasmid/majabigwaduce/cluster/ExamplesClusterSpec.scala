package com.phasmid.majabigwaduce.cluster

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.Logger as LogbackLogger
import ch.qos.logback.core.read.ListAppender
import com.phasmid.majabigwaduce.core.*
import com.phasmid.majabigwaduce.dd.{DDContext, DataDefinition, LazyDD}
import com.phasmid.majabigwaduce.examples.matrix.MatrixOperation
import com.phasmid.majabigwaduce.matrix.{Matrix2, Product}
import com.typesafe.config.{Config, ConfigFactory}
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
import scala.jdk.CollectionConverters.*

/**
 * The matrix and DataDefinition examples, on a master node which runs no worker and a worker node.
 * Each test also checks, from the worker's log, that the worker spawned the job's reducers: otherwise
 * a job which (wrongly) ran in a non-cluster ActorSystem would pass too.
 */
class ExamplesClusterSpec extends AnyWordSpec with should.Matchers with ScalaFutures with BeforeAndAfterAll {

  private var systems: Seq[ActorSystem[Nothing]] = Nil
  private def masterNode = systems.head

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(timeout = Span(20, Seconds))

  private val workerLog = LoggerFactory.getLogger("com.phasmid.majabigwaduce.cluster.ClusterWorker").asInstanceOf[LogbackLogger]
  private val appender = new ListAppender[ILoggingEvent]

  override def beforeAll(): Unit =
    appender.start()
    workerLog.addAppender(appender)
    systems = TestCluster.start(2)
    ClusterWorker.start(systems(1))

  override def afterAll(): Unit =
    TestCluster.stop(systems)
    workerLog.detachAppender(appender)

  /** Runs `body`, then checks that the worker spawned reducers for a Master on the master node. */
  private def onWorker[T](body: => T): T =
    appender.list.clear()
    val result = body
    val spawned = appender.list.asScala.map(_.getFormattedMessage).filter(_.startsWith("spawned"))
    spawned should not be empty
    all(spawned) should include(TestCluster.address(masterNode).toString)
    result

  private def ddContext: DDContext =
    DDContext(ConfigFactory.load.getConfig("majabigwaduce.DataDefinition"), masterNode, Timeout(15.seconds))

  "MatrixOperation" should {
    "multiply a matrix by a vector with its reducers on the worker node" in onWorker {
      given system: ActorSystem[Nothing] = masterNode
      given logger: Logger = LoggerFactory.getLogger(classOf[MatrixOperation[?]])
      given config: Config = ConfigFactory.load.getConfig("majabigwaduce.Matrix")
      given timeout: Timeout = Timeout(15.seconds)
      given ec: ExecutionContext = system.executionContext
      val op = MatrixOperation[Int](_ % 3)
      val matrix = Seq(Seq(1, 2, 3), Seq(4, 5, 6), Seq(7, 8, 9), Seq(1, 0, 1))
      op(matrix, Seq(1, 1, 2)).futureValue shouldBe Seq(9, 21, 33, 3)
    }
  }

  "DataDefinition" should {
    "evaluate a LazyDD with its reducers on the worker node" in onWorker {
      given DDContext = ddContext
      val dd = LazyDD[String, Int, String, Int](Seq("a" -> 1, "b" -> 2, "a" -> 3), { case (k, v) => k -> v * 10 })(2)
      dd().futureValue shouldBe Map("a" -> 40, "b" -> 20)
    }

    "map and reduce, using the DDContext in scope" in onWorker {
      given DDContext = ddContext
      val dd = DataDefinition(Seq("x" -> 1, "y" -> 2, "z" -> 3), 2).map(DataDefinition.tupleLift(_ * 2))
      dd().futureValue shouldBe Map("x" -> 2, "y" -> 4, "z" -> 6)
      dd.reduce[Int](_ + _).futureValue shouldBe 12
      dd.clean()
    }
  }

  "Matrix2" should {
    "multiply matrices larger than the cutoff with its reducers on the worker node" in onWorker {
      given DDContext = ddContext
      given Duration = 15.seconds
      given Product[Double] with
        def product[X: Numeric, Y: Numeric](x: X, y: Y): Double = summon[Numeric[X]].toDouble(x) * summon[Numeric[Y]].toDouble(y)
      given Monoid[Seq[Double]] with
        def combine(x: Seq[Double], y: Seq[Double]): Seq[Double] = x ++ y
        def zero: Seq[Double] = Nil
      val n = 25 // NOTE Matrix2's default cutoff is 20 x 20: smaller products are done without map-reduce.
      val rows = for i <- 0 until n yield for j <- 0 until n yield (i * n + j).toDouble
      Matrix2(rows).product2[Double, Double](Matrix2.identity[Double](n)).rows shouldBe rows
    }
  }
}
