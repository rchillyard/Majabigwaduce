package com.phasmid.majabigwaduce

import org.scalatest._
import org.scalatest.concurrent._
import org.scalatest.tagobjects.Slow

import scala.concurrent.duration
import scala.concurrent.duration.Duration
import scala.language.postfixOps
import scala.util.Random

class MatrixFuncSpec extends FlatSpec with Matchers with Futures with Inside {

  // TODO why does this not get satisfied from Matrix1 and Matrix2 objects?
  implicit val atMost: Duration = duration.FiniteDuration(1, "minute")

  trait DoubleProduct extends Product[Double] {
    def product[X: Numeric, Y: Numeric](x: X, y: Y): Double = implicitly[Numeric[X]].toDouble(x) * implicitly[Numeric[Y]].toDouble(y)
  }

  implicit object DoubleProduct extends DoubleProduct

  implicit object MonoidSeqDouble extends Monoid[Seq[Double]] {
    def combine(x: Seq[Double], y: Seq[Double]): Seq[Double] = x ++ y

    def zero: Seq[Double] = Nil
  }

  behavior of "Matrix2"
  it should "implement product by identity correctly" taggedAs(Slow) in {
    val r = Random
    // TODO Try to understand why this cannot multiply matrices of size 1000 (perhaps because of timeout within MapReduce code?)
    val size = 500
    val array = for (_ <- 1 to size) yield for (_ <- 1 to size) yield r.nextDouble()
    //given
    val target = Matrix2(array)
    // when
    val matrix: Matrix[Seq[Double]] = target.product2(Matrix2.identity[Double](size))
    val rows = matrix.rows
    // then
    rows shouldBe array
  }

}



