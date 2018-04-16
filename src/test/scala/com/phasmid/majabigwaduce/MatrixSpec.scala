package com.phasmid.majabigwaduce

import org.scalatest._
import org.scalatest.concurrent._

import scala.language.postfixOps

class MatrixSpec extends FlatSpec with Matchers with Futures with Inside {

  trait IntProduct extends Product[Int] {
    def product[X: Numeric, Y: Numeric](x: X, y: Y): Int = implicitly[Numeric[X]].toInt(x) * implicitly[Numeric[Y]].toInt(y)
  }

  implicit object IntProduct extends IntProduct

  implicit object MonoidSeqInt extends Monoid[Seq[Int]] {
    def combine(x: Seq[Int], y: Seq[Int]): Seq[Int] = x ++ y

    def zero: Seq[Int] = Nil
  }

  behavior of "Matrix1"
  it should "implement rows correctly" in {
    //given
    val target = Matrix1(Seq(1, 2))
    // when
    val rows = target.rows
    // then
    rows.length shouldBe 2
    rows.head shouldBe 1
    rows.last shouldBe 2
  }

  it should "implement product correctly" in {
    //given
    val target = Matrix1(Seq(1, 2))
    // when
    val matrix: Matrix[Int] = target.product(Seq(3))
    val rows = matrix.rows
    // then
    rows.length shouldBe 2
    rows.head shouldBe 3
    rows.last shouldBe 6
  }

  it should "throw exception as appropriate in product" in {
    //given
    val target = Matrix1(Seq(1, 2))
    // when
    a[IncompatibleDimensionsException] should be thrownBy target.product(Seq(3, 4))
  }

  behavior of "Matrix2"
  it should "implement rows correctly" in {
    //given
    val target = Matrix2(Seq(Seq(1, 2), Seq(3, 4)))
    // when
    val rows = target.rows
    // then
    rows.length shouldBe 2
    rows.head shouldBe Seq(1, 2)
    rows.last shouldBe Seq(3, 4)
  }

  it should "implement product correctly" in {
    //given
    val target = Matrix2(Seq(Seq(1, 2), Seq(3, 4)))
    // when
    val matrix: Matrix[Int] = target.product(Seq(-1, 1))
    val rows = matrix.rows
    // then
    rows.length shouldBe 2
    rows.head shouldBe 1
    rows.last shouldBe 1
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
    rows.length shouldBe 2
    rows.head shouldBe Seq(4, 4)
    rows.last shouldBe Seq(10, 8)
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
    rows.head.length shouldBe 2
    rows.head shouldBe Seq(5, -4)
    rows.last shouldBe Seq(4, 5)
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
    rows.head.length shouldBe 3
    rows.head shouldBe Seq(11, 12, 26)
    rows.last shouldBe Seq(7, 5, -2)
  }

  it should "throw exception as appropriate in product2" in {
    //given
    val target = Matrix2(Seq(Seq(1, 2), Seq(3, 4)))
    // when
    a[IncompatibleDimensionsException] should be thrownBy target.product2(Matrix2(Seq(Seq(3))))
  }

  it should "implement product correctly using actors" in {
    //given
    Matrix.cutoff = 10 // TODO reset this to 3 to invoke actors
    //given
    val target = Matrix2(Seq(Seq(8, 3, 2), Seq(1, -2, 4), Seq(6, 0, 5)))
    // when
    val multiplicand = Matrix2(Seq(Seq(4, 0, 10), Seq(6, 10, 0), Seq(10, 0, 34)))
    val matrix: Matrix[Seq[Int]] = target.product2(multiplicand)
    val rows = matrix.rows
    // then
    rows.length shouldBe 3
    rows.head.length shouldBe 3
    rows.head shouldBe Seq(70, 30, 148)
    rows.last shouldBe Seq(74, 0, 230)
  }
}


object MatrixSpec {
}
