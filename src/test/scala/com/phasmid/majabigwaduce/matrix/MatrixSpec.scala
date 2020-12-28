/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce.matrix

import com.phasmid.majabigwaduce.core.Monoid
import org.scalatest._
import org.scalatest.concurrent._
import org.scalatest.matchers.should

import scala.concurrent.duration
import scala.concurrent.duration.Duration
import scala.language.postfixOps

class MatrixSpec extends flatspec.AnyFlatSpec with should.Matchers with Futures with Inside {

  // TODO why does this not get satisfied from Matrix1 and Matrix2 objects?
  implicit val atMost: Duration = duration.FiniteDuration(1, "second")

  trait IntProduct extends Product[Int] {
    def product[X: Numeric, Y: Numeric](x: X, y: Y): Int = implicitly[Numeric[X]].toInt(x) * implicitly[Numeric[Y]].toInt(y)
  }

  implicit object IntProduct extends IntProduct

  implicit object MonoidSeqInt extends Monoid[Seq[Int]] {
    def combine(x: Seq[Int], y: Seq[Int]): Seq[Int] = x ++ y

    def zero: Seq[Int] = Nil
  }

  behavior of "Matrix1"
  it should "get size correctly" in {
    //given
    val target = Matrix1(Seq(1, 2))
    // when
    val size = target.size
    // then
    size.size shouldBe 1
    size.rows shouldBe 2
  }

  it should "implement rows correctly" in {
    //given
    val target = Matrix1(Seq(1, 2))
    // when
    val rows = target.rows
    // then
    rows.length shouldBe 2
    rows.headOption shouldBe Some(1)
    rows.lastOption shouldBe Some(2)
  }

  it should "implement product correctly" in {
    //given
    val target = Matrix1(Seq(1, 2))
    // when
    val matrix: Matrix[Int] = target.product(Seq(3))
    val rows = matrix.rows
    // then
    rows.length shouldBe 2
    rows.headOption shouldBe Some(3)
    rows.lastOption shouldBe Some(6)
  }

  it should "throw exception as appropriate in product" in {
    //given
    val target = Matrix1(Seq(1, 2))
    // when
    a[IncompatibleDimensionsException] should be thrownBy target.product(Seq(3, 4))
  }

  behavior of "Matrix2"
  it should "get size correctly" in {
    //given
    val target = Matrix2(Seq(Seq(1, 2, -1), Seq(3, 4, 0)))
    // when
    val size = target.size
    // then
    size.size shouldBe 2
    size.rows shouldBe 2
    size.cols shouldBe 3
  }

  it should "implement rows correctly" in {
    //given
    val target = Matrix2(Seq(Seq(1, 2), Seq(3, 4)))
    // when
    val rows = target.rows
    // then
    rows.length shouldBe 2
    rows.headOption shouldBe Some(Seq(1, 2))
    rows.lastOption shouldBe Some(Seq(3, 4))
  }

  it should "implement transpose correctly" in {
    //given
    val target = Matrix2(Seq(Seq(1, 2), Seq(3, 4)))
    // when
    val rows = target.transpose
    // then
    rows.length shouldBe 2
    rows.headOption shouldBe Some(Seq(1, 3))
    rows.lastOption shouldBe Some(Seq(2, 4))
  }

  it should "implement product correctly" in {
    //given
    val target = Matrix2(Seq(Seq(1, 2), Seq(3, 4)))
    // when
    val matrix: Matrix[Int] = target.product(Seq(-1, 1))
    val rows = matrix.rows
    // then
    rows.length shouldBe 2
    rows.headOption shouldBe Some(1)
    rows.lastOption shouldBe Some(1)
  }

  it should "throw exception as appropriate in product" in {
    //given
    val target = Matrix2(Seq(Seq(1, 2), Seq(3, 4)))
    // when
    a[IncompatibleDimensionsException] should be thrownBy target.product(Seq(3))
  }

  it should "implement product2 correctly 2x2 by 2x2" in {
    //given
    val target = Matrix2(Seq(Seq(1, 2), Seq(3, 4)))
    // when
    val multiplicand = Matrix2(Seq(Seq(2, 0), Seq(1, 2)))
    val matrix: Matrix[Seq[Int]] = target.product2(multiplicand)
    val rows = matrix.rows
    // then
    rows.headOption shouldBe Some(Seq(4, 4))
    rows.lastOption shouldBe Some(Seq(10, 8))
  }

  it should "implement product by identity correctly" in {
    val array = Seq(Seq(1, 2), Seq(3, 4))
    //given
    val target = Matrix2(array)
    // when
    val matrix: Matrix[Seq[Int]] = target.product2(Matrix2.identity[Int](2))
    val rows = matrix.rows
    // then
    rows shouldBe array
  }

  it should "implement product2 correctly 2x3 by 3x2" in {
    //given
    val target = Matrix2(Seq(Seq(1, 2, -1), Seq(2, 0, 1)))
    // when
    val multiplicand = Matrix2(Seq(Seq(3, 1), Seq(0, -1), Seq(-2, 3)))
    val matrix: Matrix[Seq[Int]] = target.product2(multiplicand)
    val rows = matrix.rows
    // then
    rows.length shouldBe 2
    rows.headOption shouldBe Some(Seq(5, -4))
    rows.lastOption shouldBe Some(Seq(4, 5))
  }

  it should "implement product2 correctly 2x3 by 3x3" in {
    //given
    val target = Matrix2(Seq(Seq(2, 3, 1), Seq(2, -7, 4)))
    // when
    val multiplicand = Matrix2(Seq(Seq(3, 4, 5), Seq(1, 1, 4), Seq(2, 1, 4)))
    val matrix: Matrix[Seq[Int]] = target.product2(multiplicand)
    val rows = matrix.rows
    // then
    rows.length shouldBe 2
    rows.headOption shouldBe Some(Seq(11, 12, 26))
    rows.lastOption shouldBe Some(Seq(7, 5, -2))
  }

  it should "throw exception as appropriate in product2" in {
    //given
    val target = Matrix2(Seq(Seq(1, 2), Seq(3, 4)))
    // when
    a[IncompatibleDimensionsException] should be thrownBy target.product2(Matrix2(Seq(Seq(3))))
  }

  it should "implement product correctly using actors" in {
    //given
    implicit val cutoff: Dimensions = Dimensions(Seq(1, 1))
    //given
    val target = Matrix2(Seq(Seq(8, 3, 2), Seq(1, -2, 4), Seq(6, 0, 5)))
    // when
    val multiplicand = Matrix2(Seq(Seq(4, 0, 10), Seq(6, 10, 0), Seq(10, 0, 34)))
    val matrix: Matrix[Seq[Int]] = target.product2(multiplicand)
    // then
    val rows = matrix.rows
    rows.length shouldBe 3
    rows.headOption shouldBe Some(Seq(70, 30, 148))
    rows.lastOption shouldBe Some(Seq(74, 0, 230))
  }
}
