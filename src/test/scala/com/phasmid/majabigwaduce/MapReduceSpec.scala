package com.phasmid.majabigwaduce

import org.scalatest._
import org.scalatest.concurrent._

import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps

class MapReduceSpec extends FlatSpec with Matchers with Futures with ScalaFutures with Inside {

  import scala.concurrent.ExecutionContext.Implicits.global

  behavior of "MapReduce"
  it should "apply correctly" in {
    val target = MockMapReduce[Int, String, Int](xs => (for (x <- xs) yield (x.toString, x)).toMap)
    val mf = target(Seq(1,2))
    val tf = mf map (_.unzip)
    whenReady(tf) { u => u should matchPattern { case (Seq("1","2"),Seq(1,2)) => } }
  }

  it should "compose correctly using MapReduceComposed" in {
    val mr1 = MockMapReduce[Int, String, Int](xs => (for (x <- xs) yield (x.toString, x)).toMap)
    def mr2(wIs: Seq[(String,Int)]): Future[Map[Int,Int]] = Future((for (wi <- wIs) yield (wi._1.toInt,wi._2)).toMap)
    val target = MapReduceComposed(mr1,mr2)
    val mf = target(Seq(1,2))
    val tf = mf map (_.unzip)
    whenReady(tf) { u => u should matchPattern { case (Seq(1,2),Seq(1,2)) => } }
  }

  it should "compose correctly with &" in {
    val mr1 = MockMapReduce[Int, String, Int](xs => (for (x <- xs) yield (x.toString, x)).toMap)
    def mr2(wIs: Seq[(String,Int)]): Future[Map[Int,Int]] = Future((for (wi <- wIs) yield (wi._1.toInt,wi._2)).toMap)
    val target = mr1 & mr2
    val mf = target(Seq(1,2))
    val tf = mf map (_.unzip)
    whenReady(tf) { u => u should matchPattern { case (Seq(1,2),Seq(1,2)) => } }
  }

  it should "terminate correctly" in {
    val mr: MockMapReduce[Int, String, Int] = MockMapReduce[Int, String, Int](xs => (for (x <- xs) yield (x.toString, x)).toMap)
    def reduce(wIm: Map[String,Int]): Int = wIm.values.sum
    val target = mr terminate reduce
  }

  it should "terminate correctly with |" in {
    val mr: MockMapReduce[Int, String, Int] = MockMapReduce[Int, String, Int](xs => (for (x <- xs) yield (x.toString, x)).toMap)
    def reduce(wIm: Map[String,Int]): Int = wIm.values.sum
    val target = mr | reduce
  }
}

case class MockMapReduce[T,K,V](f: Seq[T]=>Map[K,V]) extends MapReduce[T,K,V] {
  import scala.concurrent.ExecutionContext.Implicits.global

  def ec: ExecutionContext = implicitly[ExecutionContext]

  override def apply(v1: Seq[T]): Future[Map[K, V]] = Future(f(v1))
}

object MapReduceSpec {
}
