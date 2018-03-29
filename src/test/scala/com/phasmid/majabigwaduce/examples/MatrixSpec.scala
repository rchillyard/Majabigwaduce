package com.phasmid.majabigwaduce.examples

import com.phasmid.majabigwaduce.examples.matrix.Matrix
import org.scalamock.scalatest.MockFactory
import org.scalatest._
import org.scalatest.concurrent._

class MatrixSpec extends FlatSpec with Matchers with Futures with ScalaFutures with Inside with MockFactory {
  behavior of "dot"
  it should "work for empty and unequal vectors" in {
    Matrix.dot(Seq(),Seq(0)) shouldBe 0
    Matrix.dot[Int](Seq(),Seq()) shouldBe 0
    Matrix.dot(Seq(0),Seq(1)) shouldBe 0
  }
  it should "work for single-element vectors" in {
    Matrix.dot(Seq(1),Seq(1)) shouldBe 1
    Matrix.dot(Seq(1),Seq(0)) shouldBe 0
    Matrix.dot(Seq(0),Seq(1)) shouldBe 0
    Matrix.dot(Seq(1),Seq(2)) shouldBe 2
    Matrix.dot(Seq(2),Seq(1)) shouldBe 2
    Matrix.dot(Seq(2),Seq(2)) shouldBe 4
  }
  it should "work for multi-element vectors" in {
    Matrix.dot(Seq(1,1),Seq(1,1)) shouldBe 2
    Matrix.dot(Seq(1,0),Seq(0,1)) shouldBe 0
    Matrix.dot(Seq(1,-1),Seq(-1,1)) shouldBe -2
    Matrix.dot(Seq(1,-1),Seq(1,1)) shouldBe 0
    Matrix.dot(Seq(1,2),Seq(1,2)) shouldBe 5
  }
}


