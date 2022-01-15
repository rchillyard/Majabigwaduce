/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce.dd

import akka.util.Timeout
import com.phasmid.majabigwaduce.dd.DataDefinition._
import org.scalatest._
import org.scalatest.concurrent._
import org.scalatest.matchers.should

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.language.postfixOps

class DataDefinitionSpec extends flatspec.AnyFlatSpec with should.Matchers with Futures with ScalaFutures with Inside {

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
    val xf: Future[Int] = target.reduce[Int](_ + _)
    // then
    whenReady(xf) { x => x should matchPattern { case 3 => } }
    target.clean()
  }

  it should "aggregate correctly with multiple partitions" in {
    // given
    val target = DataDefinition(Map("a" -> 1, "b" -> 2))
    // when
    val xf: Future[Int] = target.reduce[Int](_ + _)
    // then
    whenReady(xf) { x => x should matchPattern { case 3 => } }
    target.clean()
  }

  it should "count correctly with single partition" in {
    // given
    val target = DataDefinition(Map("a" -> 1, "b" -> 2), 0)
    // when
    val xf: Future[Int] = target.count
    // then
    whenReady(xf) { x => x should matchPattern { case 2 => } }
    target.clean()
  }

  it should "count correctly with multiple partition" in {
    // given
    val target = DataDefinition(Map("a" -> 1, "b" -> 2))
    // when
    val xf: Future[Int] = target.count
    // then
    whenReady(xf) { x => x should matchPattern { case 2 => } }
    target.clean()
  }

  it should "filter correctly with single partition" in {
    // given
    val target = DataDefinition(Map("a" -> 1, "b" -> 2), 0)
    // when
    val xf: Future[Int] = target.filter(x => x._1 == "a").count
    // then
    whenReady(xf) { x => x should matchPattern { case 1 => } }
    target.clean()
  }

  it should "filter correctly with multiple partition" in {
    // given
    val target = DataDefinition(Map("a" -> 1, "b" -> 2))
    // when
    val xf: Future[Int] = target.filter(x => x._1 == "a").count
    // then
    whenReady(xf) { x => x should matchPattern { case 1 => } }
    target.clean()
  }

  it should "filter by value correctly with single partition" in {
    // given
    val target = DataDefinition(Map("a" -> 1, "b" -> 2, "c" -> 3), 0)
    // when
    val xf: Future[Int] = target.filter(x => x._2 % 2 == 0).count
    // then
    whenReady(xf) { x => x should matchPattern { case 1 => } }
    target.clean()
  }

  it should "groupBy correctly with single partition" in {
    // given
    val target = DataDefinition(Map(1 -> 1, 2 -> 2, 3 -> -1), 0)
    // when
    val xf: Future[Map[Boolean, Iterable[Int]]] = target.groupBy(x => x > 0)()
    // then
    whenReady(xf) { x => x(true) should matchPattern { case _ :: _ :: Nil => } }
    target.clean()
  }

  it should "join/count correctly with single partition" in {
    // given
    val target = DataDefinition(Map("a" -> 1, "b" -> 2), 0)
    val target2 = DataDefinition(Map("a" -> 2.1, "c" -> 3.1), 0)
    // when
    val xf: Future[Int] = target.join(target2).count
    // then
    whenReady(xf) { x => x should matchPattern { case 1 => } }
    target.clean()
  }

  it should "join/count correctly with multiple partition" in {
    // given
    val target = DataDefinition(Map("a" -> 1, "b" -> 2))
    val target2 = DataDefinition(Map("a" -> 2.1, "c" -> 3.1))
    // when
    val xf: Future[Int] = target.join(target2).count
    // then
    whenReady(xf) { x => x should matchPattern { case 1 => } }
    target.clean()
  }

  it should "join/aggregate correctly with single partition" in {
    // given
    val target = DataDefinition(Map("a" -> 1, "b" -> 2, "c" -> 3), 0)
    val target2 = DataDefinition(Map("a" -> 2.1, "b" -> 3.1), 0)
    // when
    val xf: Future[(Int, Double)] = target.join(target2).reduce[(Int, Double)]((x, y) => (x._1 + y._1, x._2 + y._2))
    // then
    whenReady(xf) { x => x should matchPattern { case (3, 5.2) => } }
    target.clean()
  }

  it should "join/aggregate correctly with multiple partition" in {
    // given
    val target = DataDefinition(Map("a" -> 1, "b" -> 2, "c" -> 3))
    val target2 = DataDefinition(Map("a" -> 2.1, "b" -> 3.1))
    // when
    val xf: Future[(Int, Double)] = target.join(target2).reduce[(Int, Double)]((x, y) => (x._1 + y._1, x._2 + y._2))
    // then
    whenReady(xf) { x => x should matchPattern { case (3, 5.2) => } }
    target.clean()
  }

  it should "join/chain/aggregate correctly with single partition" in {
    // given
    val target = DataDefinition(Map("a" -> 1, "b" -> 2, "c" -> 3), 0)
    val target2 = DataDefinition(Map("a" -> 2.1, "b" -> 3.1), 0)
    val target3 = DataDefinition(Map("a" -> "Hello", "b" -> "World"), 0)
    // when
    val xf: Future[((Int, Double), String)] = target.join(target2).join(target3).reduce[((Int, Double), String)]((x, y) => ((x._1._1 + y._1._1, x._1._2 + y._1._2), x._2 + "," + y._2))
    // then
    whenReady(xf) { x => x should matchPattern { case ((3, 5.2), ",Hello,World") => } }
    target.clean()
  }

  it should "join/chain/aggregate correctly with multiple partition" in {
    // given
    val target = DataDefinition(Map("a" -> 1, "b" -> 2, "c" -> 3))
    val target2 = DataDefinition(Map("a" -> 2.1, "b" -> 3.1))
    val target3 = DataDefinition(Map("a" -> "Hello", "b" -> "World"))
    // when
    val xf: Future[((Int, Double), String)] = target.join(target2).join(target3).reduce[((Int, Double), String)]((x, y) => ((x._1._1 + y._1._1, x._1._2 + y._1._2), x._2 + "," + y._2))
    // then
    whenReady(xf) { x => x should matchPattern { case ((3, 5.2), ",Hello,World") => case ((3, 5.2), ",World,Hello") => } }
    target.clean()
  }

  it should "map/join/aggregate correctly with single partition" in {
    // given
    val target = DataDefinition(Map("a" -> 1, "b" -> 2, "c" -> 3), 0)
    val target2 = DataDefinition(Map("a" -> 2.1, "b" -> 3.1), 0)
    // when
    val xf: Future[(Int, Double)] = target.map(tupleLift(_ * 2)).join(target2).reduce[(Int, Double)]((x, y) => (x._1 + y._1, x._2 + y._2))
    // then
    whenReady(xf) { x => x should matchPattern { case (6, 5.2) => } }
    target.clean()
  }

  it should "map/join/aggregate correctly with multiple partition" in {
    // given
    val target = DataDefinition(Map("a" -> 1, "b" -> 2, "c" -> 3))
    val target2 = DataDefinition(Map("a" -> 2.1, "b" -> 3.1))
    // when
    val xf: Future[(Int, Double)] = target.map(tupleLift(_ * 2)).join(target2).reduce[(Int, Double)]((x, y) => (x._1 + y._1, x._2 + y._2))
    // then
    whenReady(xf) { x => x should matchPattern { case (6, 5.2) => } }
    target.clean()
  }

  it should "map/join/aggregate correctly with EagerDD" in {
    // given
    val target = DataDefinition(Map("a" -> 1, "b" -> 2, "c" -> 3))
    val target2 = EagerDD(Map("a" -> 2.1, "b" -> 3.1))
    // when
    val xf: Future[(Int, Double)] = target.map(tupleLift(_ * 2)).join(target2).reduce[(Int, Double)]((x, y) => (x._1 + y._1, x._2 + y._2))
    // then
    whenReady(xf) { x => x should matchPattern { case (6, 5.2) => } }
    target.clean()
  }

  // Closed issue #7
  //The join only works when key is never changed, this test won't work since key changes
  //Resolution:
  // Issue 1: For the issue of the sequence not being maintained, changed the type of kWsm from mutable.HashMap
  // to mutable.LinkedHashMap. HashMap fails to preserve the order when the keys are updated. LinkedHashMap preserves the sequence.
  // Issue 2: When the two LazyDDs, A and B, were joined and one of the LazyDD has undergone a change in key, lets say A,
  // the join used to take place on the original keys of both the LazyDDs A and B and not between the updated key of A and original key of B.
  // This was resolved by adding a new function "joinMap2" which joins the two LazyDDs on the basis of new keys, but stores
  // the resultant LazyDD with the old keys. This has been done because in function apply(), we call the function evaluate
  // which uses the combination of two anonymous functions given by the function "joinFunction". If the keys in resultant RDD
  // had new keys, it would be rerunning the same function.
  it should "mapKeyAndValue/join/apply correctly with single partition" in {
    // given
    val target = DataDefinition(Map("a" -> 1, "b" -> 2, "c" -> 3), 0)
    val target2 = DataDefinition(Map("a1" -> 2.1, "b1" -> 3.1), 0)
    // when
    val mf: Future[Map[String, Int]] = target.map(x => (x._1 + "1", x._2 * 2)).apply()
    val mjf: Future[Map[String, (Int, Double)]] = target.map(x => (x._1 + "1", x._2 * 2)).join(target2).apply()
    // then
    whenReady(mf) { m => m.toSeq should matchPattern { case Seq(("a1", 2), ("b1", 4), ("c1", 6)) => } }
    whenReady(mjf) { m => m.toSeq should matchPattern { case Seq(("a1", (2, 2.1)), ("b1", (4, 3.1))) => } }
    target.clean()
  }

  it should "map/apply correctly with single partition" in {
    // given
    val target = DataDefinition(Map("a" -> 1, "b" -> 2), 0)
    // when
    val mf: Future[Map[String, Int]] = target.map(tupleLift(_ * 2)).apply()
    // then
    whenReady(mf) { m => m.toSeq should matchPattern { case Seq(("a", 2), ("b", 4)) => } }
    target.clean()
  }

  it should "map/apply correctly with multiple partitions" in {
    // given
    val target = DataDefinition(Map("a" -> 1, "b" -> 2))
    // when
    val mf: Future[Map[String, Int]] = target.map(tupleLift(_ * 2)).apply()
    // then
    import scala.concurrent.duration._
    implicit val timeout: Timeout = Timeout(5 seconds)
    whenReady(mf) { m => m.values.sum shouldBe 6 }
    target.clean()
  }

  behavior of "LazyDD of Seq"
  it should "apply correctly with single partition" in {
    // given
    def mapper(w: String): Int = w.charAt(0).toInt - 'a'.toInt + 1

    val target = DataDefinition(Seq("a", "b"), mapper, 1)
    // when
    val mf: Future[Map[Int, String]] = target()
    // then
    whenReady(mf) { m => m.toSeq should matchPattern { case Seq((1, "a"), (2, "b")) => } }
    target.clean()
  }

  it should "apply correctly with multiple partitions" in {
    // given
    def mapper(w: String): Int = w.charAt(0).toInt - 'a'.toInt + 1

    val target = DataDefinition.create(Seq("a", "b"), mapper)
    // when
    val mf: Future[Map[Int, String]] = target()
    // then
    import scala.concurrent.duration._
    implicit val timeout: Timeout = Timeout(2 seconds)
    whenReady(mf) { m => m.toSeq.size shouldBe 2 }
    target.clean()
  }

  behavior of "EagerDD of Map"
  it should "apply correctly with single partition" in {
    // given
    val target = EagerDD(Map("a" -> 1, "b" -> 2))
    // when
    val mf: Future[Map[String, Int]] = target()
    // then
    whenReady(mf) { m => m.toSeq should matchPattern { case Seq(("a", 1), ("b", 2)) => } }
    target.clean()
  }

  it should "apply correctly with multiple partitions" in {
    // given
    val target = EagerDD(Map("a" -> 1, "b" -> 2))
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
    val target = EagerDD(Map("a" -> 1, "b" -> 2))
    // when
    val xf: Future[Int] = target.reduce[Int](_ + _)
    // then
    whenReady(xf) { x => x should matchPattern { case 3 => } }
    target.clean()
  }

  it should "aggregate correctly with multiple partitions" in {
    // given
    val target = EagerDD(Map("a" -> 1, "b" -> 2))
    // when
    val xf: Future[Int] = target.reduce[Int](_ + _)
    // then
    whenReady(xf) { x => x should matchPattern { case 3 => } }
    target.clean()
  }

  it should "count correctly with single partition" in {
    // given
    val target = EagerDD(Map("a" -> 1, "b" -> 2))
    // when
    val xf: Future[Int] = target.count
    // then
    whenReady(xf) { x => x should matchPattern { case 2 => } }
    target.clean()
  }

  it should "count correctly with multiple partition" in {
    // given
    val target = EagerDD(Map("a" -> 1, "b" -> 2))
    // when
    val xf: Future[Int] = target.count
    // then
    whenReady(xf) { x => x should matchPattern { case 2 => } }
    target.clean()
  }

  it should "filter correctly with single partition" in {
    // given
    val target = EagerDD(Map("a" -> 1, "b" -> 2))
    // when
    val xf: Future[Int] = target.filter(x => x._1 == "a").count
    // then
    whenReady(xf) { x => x should matchPattern { case 1 => } }
    target.clean()
  }

  it should "filter correctly with multiple partition" in {
    // given
    val target = EagerDD(Map("a" -> 1, "b" -> 2))
    // when
    val xf: Future[Int] = target.filter(x => x._1 == "a").count
    // then
    whenReady(xf) { x => x should matchPattern { case 1 => } }
    target.clean()
  }

  it should "groupBy correctly with single partition" in {
    // given
    val target = EagerDD(Map(1 -> 1, 2 -> 2, 3 -> -1))
    // when
    val xf: Future[Map[Boolean, Iterable[Int]]] = target.groupBy(x => x > 0)()
    // then
    whenReady(xf) { x => x(true) should matchPattern { case _ :: _ :: Nil => } }
    target.clean()
  }

  it should "join/count correctly with single partition" in {
    // given
    val target = EagerDD(Map("a" -> 1, "b" -> 2))
    val target2 = EagerDD(Map("a" -> 2.1, "c" -> 3.1))
    // when
    val xf: Future[Int] = target.join(target2).count
    // then
    whenReady(xf) { x => x should matchPattern { case 1 => } }
    target.clean()
  }

  it should "join/count correctly with multiple partition" in {
    // given
    val target = EagerDD(Map("a" -> 1, "b" -> 2))
    val target2 = EagerDD(Map("a" -> 2.1, "c" -> 3.1))
    // when
    val xf: Future[Int] = target.join(target2).count
    // then
    whenReady(xf) { x => x should matchPattern { case 1 => } }
    target.clean()
  }

  it should "join/count correctly with LazyDD" in {
    // given
    val target = EagerDD(Map("a" -> 1, "b" -> 2))
    val target2 = DataDefinition(Map("a" -> 2.1, "c" -> 3.1))
    // when
    val xf: Future[Int] = target.join(target2).count
    // then
    whenReady(xf) { x => x should matchPattern { case 1 => } }
    target.clean()
  }

  it should "join/aggregate correctly with single partition" in {
    // given
    val target = EagerDD(Map("a" -> 1, "b" -> 2, "c" -> 3))
    val target2 = EagerDD(Map("a" -> 2.1, "b" -> 3.1))
    // when
    val xf: Future[(Int, Double)] = target.join(target2).reduce[(Int, Double)]((x, y) => (x._1 + y._1, x._2 + y._2))
    // then
    whenReady(xf) { x => x should matchPattern { case (3, 5.2) => } }
    target.clean()
  }

  it should "join/aggregate correctly with multiple partition" in {
    // given
    val target = EagerDD(Map("a" -> 1, "b" -> 2, "c" -> 3))
    val target2 = EagerDD(Map("a" -> 2.1, "b" -> 3.1))
    // when
    val xf: Future[(Int, Double)] = target.join(target2).reduce[(Int, Double)]((x, y) => (x._1 + y._1, x._2 + y._2))
    // then
    whenReady(xf) { x => x should matchPattern { case (3, 5.2) => } }
    target.clean()
  }

  it should "join/chain/aggregate correctly with single partition" in {
    // given
    val target = EagerDD(Map("a" -> 1, "b" -> 2, "c" -> 3))
    val target2 = EagerDD(Map("a" -> 2.1, "b" -> 3.1))
    val target3 = EagerDD(Map("a" -> "Hello", "b" -> "World"))
    // when
    val xf: Future[((Int, Double), String)] = target.join(target2).join(target3).reduce[((Int, Double), String)]((x, y) => ((x._1._1 + y._1._1, x._1._2 + y._1._2), x._2 + "," + y._2))
    // then
    whenReady(xf) { x => x should matchPattern { case ((3, 5.2), ",Hello,World") => } }
    target.clean()
  }

  it should "join/chain/aggregate correctly with multiple partition" in {
    // given
    val target = EagerDD(Map("a" -> 1, "b" -> 2, "c" -> 3))
    val target2 = EagerDD(Map("a" -> 2.1, "b" -> 3.1))
    val target3 = EagerDD(Map("a" -> "Hello", "b" -> "World"))
    // when
    val joined = target.join(target2).join(target3)
    type Triple = ((Int, Double), String)
    val xf: Future[Triple] = joined.reduce[Triple]((x, y) => ((x._1._1 + y._1._1, x._1._2 + y._1._2), x._2 + "," + y._2))
    // then
    whenReady(xf) { x => x should matchPattern { case ((3, 5.2), ",Hello,World") => case ((3, 5.2), ",World,Hello") => } }
    target.clean()
  }

  it should "map/join/aggregate correctly with single partition" in {
    // given
    val target = EagerDD(Map("a" -> 1, "b" -> 2, "c" -> 3))
    val target2 = EagerDD(Map("a" -> 2.1, "b" -> 3.1))
    // when
    val xf: Future[(Int, Double)] = target.map(tupleLift(_ * 2)).join(target2).reduce[(Int, Double)]((x, y) => (x._1 + y._1, x._2 + y._2))
    // then
    whenReady(xf) { x => x should matchPattern { case (6, 5.2) => } }
    target.clean()
  }

  it should "map/join/aggregate correctly with multiple partition" in {
    // given
    val target = EagerDD(Map("a" -> 1, "b" -> 2, "c" -> 3))
    val target2 = EagerDD(Map("a" -> 2.1, "b" -> 3.1))
    // when
    val xf: Future[(Int, Double)] = target.map(tupleLift(_ * 2)).join(target2).reduce[(Int, Double)]((x, y) => (x._1 + y._1, x._2 + y._2))
    // then
    whenReady(xf) { x => x should matchPattern { case (6, 5.2) => } }
    target.clean()
  }

  // The join only works when key is never changed, this test won't work since key changes
  // The problem here was attributed to Issue #7 but it seems to work now.
  it should "mapKeyAndValue/join/apply correctly with single partition" in {
    // given
    val target = EagerDD(Map("a" -> 1, "b" -> 2, "c" -> 3))
    val target2 = DataDefinition(Map("a1" -> 2.1, "b1" -> 3.1), 0)
    // when
    val mf: Future[Map[String, Int]] = target.map(x => (x._1 + "1", x._2 * 2)).apply()
    val mjf: Future[Map[String, (Int, Double)]] = target.map(x => (x._1 + "1", x._2 * 2)).join(target2).apply()
    // then
    whenReady(mf) { m => m.toSeq should matchPattern { case Seq(("a1", 2), ("b1", 4), ("c1", 6)) => } }
    whenReady(mjf) { m => m.toSeq should matchPattern { case Seq(("a1", (2, 2.1)), ("b1", (4, 3.1))) => } }
    target.clean()
  }

  it should "map/apply correctly with single partition" in {
    // given
    val target = EagerDD(Map("a" -> 1, "b" -> 2))
    // when
    val mf: Future[Map[String, Int]] = target.map(tupleLift(_ * 2)).apply()
    // then
    whenReady(mf) { m => m.toSeq should matchPattern { case Seq(("a", 2), ("b", 4)) => } }
    target.clean()
  }

  it should "map/apply correctly with multiple partitions" in {
    // given
    val target = EagerDD(Map("a" -> 1, "b" -> 2))
    // when
    val mf: Future[Map[String, Int]] = target.map(tupleLift(_ * 2)).apply()
    // then
    import scala.concurrent.duration._
    implicit val timeout: Timeout = Timeout(5 seconds)
    whenReady(mf) { m => m.values.sum shouldBe 6 }
    target.clean()
  }

}
