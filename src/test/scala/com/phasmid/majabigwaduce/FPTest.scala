package com.phasmid.majabigwaduce

import com.phasmid.majabigwaduce.FP._
import org.scalatest.{flatspec, matchers}

import scala.util.{Failure, Success}

class FPTest extends flatspec.AnyFlatSpec with matchers.should.Matchers {
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
}
