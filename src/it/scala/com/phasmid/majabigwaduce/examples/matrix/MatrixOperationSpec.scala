/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce.examples.matrix

import com.phasmid.majabigwaduce.matrix.{IncompatibleLengthsException, Matrix2}
import org.scalamock.scalatest.MockFactory
import org.scalatest._
import org.scalatest.concurrent._
import org.scalatest.matchers.should

import scala.util.{Failure, Success}

class MatrixOperationSpec extends flatspec.AnyFlatSpec with should.Matchers with Futures with ScalaFutures with Inside with MockFactory {
  behavior of "dot"

  it should "work for empty vectors" in {
    MatrixOperation.dot[Int](Seq(), Seq()) shouldBe Success(0)
  }
  it should "fail for unequal vectors" in {
    MatrixOperation.dot(Seq(), Seq(0)) should matchPattern { case Failure(_) => }
  }
  it should "work for single-element vectors" in {
    MatrixOperation.dot(Seq(1), Seq(1)) shouldBe Success(1)
    MatrixOperation.dot(Seq(1), Seq(0)) shouldBe Success(0)
    MatrixOperation.dot(Seq(0), Seq(1)) shouldBe Success(0)
    MatrixOperation.dot(Seq(1), Seq(2)) shouldBe Success(2)
    MatrixOperation.dot(Seq(2), Seq(1)) shouldBe Success(2)
    MatrixOperation.dot(Seq(2), Seq(2)) shouldBe Success(4)
  }
  it should "work for multi-element vectors" in {
    MatrixOperation.dot(Seq(1, 1), Seq(1, 1)) shouldBe Success(2)
    MatrixOperation.dot(Seq(1, 0), Seq(0, 1)) shouldBe Success(0)
    MatrixOperation.dot(Seq(1, -1), Seq(-1, 1)) shouldBe Success(-2)
    MatrixOperation.dot(Seq(1, -1), Seq(1, 1)) shouldBe Success(0)
    MatrixOperation.dot(Seq(1, 2), Seq(1, 2)) shouldBe Success(5)
  }
  behavior of "product"
  it should "work for identity matrix" in {
    val vector = Seq(3, 4, 5)
    val matrix = Matrix2.identity[Int](vector.size)
    MatrixOperation.product(vector, matrix.rows) shouldBe Success(vector)
  }
  it should "fail for unequal vectors" in {
    a[IncompatibleLengthsException] shouldBe thrownBy(MatrixOperation.product(Seq(), Seq(Seq(0))).get)
  }
  it should "work when matrix has no columns, even when unequal" in {
    MatrixOperation.product(Seq(0), Seq(Seq())) shouldBe Success(Nil)
  }
  it should "work for empty and unequal vectors" in {
    MatrixOperation.product[Int](Seq(), Seq()) shouldBe Success(Nil)
  }
}


