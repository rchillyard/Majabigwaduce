/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce.core

import com.phasmid.majabigwaduce.ASync
import org.scalatest._
import org.scalatest.concurrent._
import org.scalatest.matchers.should

import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

class MapReduceSpec extends flatspec.AnyFlatSpec with should.Matchers with Futures with ScalaFutures with Inside {

  import scala.concurrent.ExecutionContext.Implicits.global

  behavior of "MapReduce"
  it should "apply correctly" in {
    val target = MockMapReduce[Int, String, Int](xs => (for (x <- xs) yield (x.toString, x)).toMap)
    val mf = target(Seq(1, 2))
    val tf = mf map (_.unzip)
    whenReady(tf) { u => u should matchPattern { case (Seq("1", "2"), Seq(1, 2)) => } }
  }

  it should "compose correctly using MapReduceComposed" in {
    val mr1 = MockMapReduce[Int, String, Int](xs => (for (x <- xs) yield (x.toString, x)).toMap)

    def mr2(wIs: Seq[(String, Int)]): Future[Map[Int, Int]] = Future((for (wi <- wIs) yield (wi._1.toInt, wi._2)).toMap)

    val target = MapReduceComposed(mr1, mr2)
    val mf = target(Seq(1, 2))
    val tf = mf map (_.unzip)
    whenReady(tf) { u => u should matchPattern { case (Seq(1, 2), Seq(1, 2)) => } }
  }

  it should "compose correctly with &" in {
    val mr1 = MockMapReduce[Int, String, Int](xs => (for (x <- xs) yield (x.toString, x)).toMap)
    val mr2 = MockMapReduce[(String, Int), Int, Int](wIs => (for (wi <- wIs) yield (wi._1.toInt, wi._2)).toMap)

    val target: MapReduce[Int, Int, Int] = mr1 & mr2
    val mf: Future[Map[Int, Int]] = target(Seq(1, 2))
    val tf = mf map (_.unzip)
    whenReady(tf) { u => u should matchPattern { case (Seq(1, 2), Seq(1, 2)) => } }
  }

  it should "compose correctly with :&" in {
    val mr1 = MockMapReduce[Int, String, Int](xs => (for (x <- xs) yield (x.toString, x)).toMap)

    def mr2(wIs: Seq[(String, Int)]): Future[Map[Int, Int]] = Future((for (wi <- wIs) yield (wi._1.toInt, wi._2)).toMap)

    val target = mr1 :& mr2
    val mf: Future[Map[Int, Int]] = target(Seq(1, 2))
    val tf = mf map (_.unzip)
    whenReady(tf) { u => u should matchPattern { case (Seq(1, 2), Seq(1, 2)) => } }
  }

  it should "terminate correctly" in {
    val mr: MockMapReduce[Int, String, Int] = MockMapReduce[Int, String, Int](xs => (for (x <- xs) yield (x.toString, x)).toMap)

    def reduce(wIm: Map[String, Int]): Int = wIm.values.sum

    val target: ASync[Seq[Int], Int] = mr :| reduce
    val mf: Future[Int] = target(Seq(1, 2))
    whenReady(mf) { u => u should matchPattern { case 3 => } }

  }

  it should "terminate correctly with |" in {
    val mr: MockMapReduce[Int, String, Int] = MockMapReduce[Int, String, Int](xs => (for (x <- xs) yield (x.toString, x)).toMap)

    def reduce(wIm: Map[String, Int]): Int = wIm.values.sum

    val target = mr | reduce
    val mf = target(Seq(1, 2))
    whenReady(mf) { u => u should matchPattern { case 3 => } }
  }
}

case class MockMapReduce[T, K, V](f: Seq[T] => Map[K, V]) extends MapReduce[T, K, V] {

  import scala.concurrent.ExecutionContext.Implicits.global

  def ec: ExecutionContext = implicitly[ExecutionContext]

  override def apply(v1: Seq[T]): Future[Map[K, V]] = Future(f(v1))

  def close(): Unit = ()
}
