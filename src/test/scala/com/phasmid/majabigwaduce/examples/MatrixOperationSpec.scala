/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce.examples

import com.phasmid.majabigwaduce.examples.matrix.MatrixOperation
import org.scalamock.scalatest.MockFactory
import org.scalatest._
import org.scalatest.concurrent._
import org.scalatest.matchers.should

class MatrixOperationSpec extends FlatSpec with should.Matchers with Futures with ScalaFutures with Inside with MockFactory {
  behavior of "dot"
  it should "work for empty and unequal vectors" in {
    MatrixOperation.dot(Seq(), Seq(0)) shouldBe 0
    MatrixOperation.dot[Int](Seq(), Seq()) shouldBe 0
    MatrixOperation.dot(Seq(0), Seq(1)) shouldBe 0
  }
  it should "work for single-element vectors" in {
    MatrixOperation.dot(Seq(1), Seq(1)) shouldBe 1
    MatrixOperation.dot(Seq(1), Seq(0)) shouldBe 0
    MatrixOperation.dot(Seq(0), Seq(1)) shouldBe 0
    MatrixOperation.dot(Seq(1), Seq(2)) shouldBe 2
    MatrixOperation.dot(Seq(2), Seq(1)) shouldBe 2
    MatrixOperation.dot(Seq(2), Seq(2)) shouldBe 4
  }
  it should "work for multi-element vectors" in {
    MatrixOperation.dot(Seq(1, 1), Seq(1, 1)) shouldBe 2
    MatrixOperation.dot(Seq(1, 0), Seq(0, 1)) shouldBe 0
    MatrixOperation.dot(Seq(1, -1), Seq(-1, 1)) shouldBe -2
    MatrixOperation.dot(Seq(1, -1), Seq(1, 1)) shouldBe 0
    MatrixOperation.dot(Seq(1, 2), Seq(1, 2)) shouldBe 5
  }
  behavior of "product"
  ignore should "work for empty and unequal vectors" in {
    MatrixOperation.product(Seq(), Seq(Seq(0))) shouldBe Seq()
    MatrixOperation.product[Int](Seq(), Seq()) shouldBe Seq()
    MatrixOperation.product(Seq(0), Seq(Seq())) shouldBe Seq()
  }
}


