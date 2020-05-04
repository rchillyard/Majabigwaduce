package com.phasmid.majabigwaduce

import com.phasmid.majabigwaduce.FP._
import org.scalatest.{flatspec, matchers}

import scala.util.{Failure, Success}

class FPSpec extends flatspec.AnyFlatSpec with matchers.should.Matchers {
  behavior of "FP"

  it should "sequence" in {
    val try1 = Success(1)
    val try2 = Success(2)
    val try3 = Failure(MapReduceException(""))
    sequence(Seq(try1, try2)) shouldBe Success(Seq(1, 2))
    sequence(Seq(try1, try3)) should matchPattern { case Failure(_) => }
  }

  it should "work for sequenceRight" in {
    val map: Map[String, Either[Int, Double]] = Map("x" -> Left(1), "y" -> Right(1.0))
    sequenceRight(map.toSeq) shouldBe Seq("y" -> 1.0)
  }

  it should "work for sequenceLeft" in {
    val map: Map[String, Either[Int, Double]] = Map("x" -> Left(1), "y" -> Right(1.0))
    sequenceLeft(map.toSeq) shouldBe Seq("x" -> 1)
  }

  behavior of "checkCompatible"
  it should "succeed for a sequence and itself" in {
    val xs = Seq(1, 2, 3)
    checkCompatible(xs, xs) should matchPattern { case Success((`xs`, `xs`)) => }
  }
  it should "fail when any parameter is Nil" in {
    val xs = Seq(1, 2, 3)
    checkCompatible(xs, Nil) should matchPattern { case Failure(_) => }
    checkCompatible(Nil, xs) should matchPattern { case Failure(_) => }
    checkCompatible(Nil, Nil) should matchPattern { case Failure(_) => }
  }
  it should "fail for a sequence and something of different length" in {
    val xs = Seq(1, 2, 3)
    checkCompatible(xs, xs take 2) should matchPattern { case Failure(_) => }
    checkCompatible(xs take 2, xs) should matchPattern { case Failure(_) => }
  }

  behavior of "checkCompatibleX"
  it should "succeed for a sequence and itself" in {
    val xs = Seq(1, 2, 3)
    val yss = Seq(Seq(1, 2, 3), Seq(1, 2, 3), Seq(1, 2, 3))
    checkCompatibleX(xs, yss) should matchPattern { case Success((`xs`, _)) => }
  }
  it should "fail when any parameter is Nil" in {
    val xs = Seq(1, 2, 3)
    val yss = Seq(Seq(1, 2, 3), Seq(1, 2, 3), Seq(1, 2, 3))
    checkCompatibleX(xs, Nil) should matchPattern { case Failure(_) => }
    checkCompatibleX(Nil, yss) should matchPattern { case Failure(_) => }
    checkCompatibleX(Nil, Nil) should matchPattern { case Failure(_) => }
  }
  it should "fail for a sequence and something of different length" in {
    val xs = Seq(1, 2, 3)
    val yss = Seq(Seq(1, 2), Seq(1, 2), Seq(1, 2))
    checkCompatibleX(xs, yss) should matchPattern { case Failure(_) => }
  }
}
