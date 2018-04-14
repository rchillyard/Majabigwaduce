package com.phasmid.majabigwaduce

import akka.util.Timeout
import org.scalatest._
import org.scalatest.concurrent._

import scala.concurrent.Future
import scala.language.postfixOps

class DataDefinitionSpec extends FlatSpec with Matchers with Futures with ScalaFutures with Inside {

  behavior of "LazyDD of Map"
  it should "apply correctly with single partition" in {
    // given
    val target = DataDefinition(Map("a" -> 1, "b" -> 2), 0)
    // when
    val mf: Future[Map[String, Int]] = target()
    // then
    whenReady(mf) { m => m.toSeq should matchPattern { case Seq(("a", 1), ("b", 2)) => } }
    target.clean()
  }

  it should "apply correctly with multiple partitions" in {
    // given
    val target = DataDefinition(Map("a" -> 1, "b" -> 2))
    // when
    val mf: Future[Map[String, Int]] = target()
    // then
    import scala.concurrent.duration._
    implicit val timeout: Timeout = Timeout(5 seconds)
    whenReady(mf) { m => m.toSeq.size shouldBe 2 }
    target.clean()
  }

  it should "aggregate correctly with single partition" in {
    // given
    val target = DataDefinition(Map("a" -> 1, "b" -> 2), 0)
    // when
    val xf: Future[Int] = target.aggregate[Int](_ + _)
    // then
    whenReady(xf) { x => x should matchPattern { case 3 => } }
    target.clean()
  }

  it should "aggregate correctly with multiple partitions" in {
    // given
    val target = DataDefinition(Map("a" -> 1, "b" -> 2))
    // when
    val xf: Future[Int] = target.aggregate[Int](_ + _)
    // then
    whenReady(xf) { x => x should matchPattern { case 3 => } }
    target.clean()
  }

  behavior of "LazyDD of Seq"
  it should "apply correctly with single partition" in {
    // given
    def mapper(w: String): Int = w.charAt(0).toInt - 'a'.toInt + 1

    val target = DataDefinition(Seq("a", "b"), mapper, 0)
    // when
    val mf: Future[Map[Int, String]] = target()
    // then
    whenReady(mf) { m => m.toSeq should matchPattern { case Seq((1, "a"), (2, "b")) => } }
    target.clean()
  }

  it should "apply correctly with multiple partitions" in {
    // given
    def mapper(w: String): Int = w.charAt(0).toInt - 'a'.toInt + 1

    val target = DataDefinition(Seq("a", "b"), mapper _)
    // when
    val mf: Future[Map[Int, String]] = target()
    // then
    import scala.concurrent.duration._
    implicit val timeout: Timeout = Timeout(5 seconds)
    whenReady(mf) { m => m.toSeq.size shouldBe 2 }
    target.clean()
  }

}
