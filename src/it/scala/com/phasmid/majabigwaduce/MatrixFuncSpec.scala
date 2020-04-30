/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce

import org.scalatest._
import org.scalatest.concurrent._
import org.scalatest.matchers.should
import org.scalatest.tagobjects.Slow

import scala.concurrent.duration
import scala.concurrent.duration.Duration
import scala.language.postfixOps
import scala.util.Random

class MatrixFuncSpec extends flatspec.AnyFlatSpec with should.Matchers with Futures with Inside {

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

  it should "implement product by identity correctly (N=500)" taggedAs Slow in {
    productByIdentity(500)
  }

  // FIXME Issue #5 cannot handle 1000 elements
  ignore should "implement product by identity correctly (N=1000)" taggedAs Slow in {
    productByIdentity(1000)
  }

  private def productByIdentity(N: Int): Unit = {
    val r = Random
    val size = N
    // CONSIDER removing the toVector as it doesn't really seem to make any difference
    val array = for (_ <- (1 to size).toVector) yield for (_ <- (1 to size).toVector) yield r.nextDouble()
    //given
    val target = Matrix2(array)
    // when
    val start = System.currentTimeMillis()
    val matrix: Matrix[Seq[Double]] = target.product2(Matrix2.identity[Double](size))
    val end = System.currentTimeMillis()
    val rows = matrix.rows
    // then
    rows shouldBe array
    println(s"time to multiply matrix of size $size by $size by the identity matrix is: ${end - start} mSecs")
  }
}



