/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce

import com.phasmid.majabigwaduce.core.Monoid
import com.phasmid.majabigwaduce.matrix.{Matrix, Matrix2, Product}
import org.scalatest.*
import org.scalatest.concurrent.*
import org.scalatest.matchers.should
import org.scalatest.tagobjects.Slow

import scala.concurrent.duration
import scala.concurrent.duration.Duration
import scala.util.{Random, Try}

class MatrixFuncSpec extends flatspec.AnyFlatSpec with should.Matchers with Futures with Inside {

  trait DoubleProduct extends Product[Double] {
    def product[X: Numeric, Y: Numeric](x: X, y: Y): Double = summon[Numeric[X]].toDouble(x) * summon[Numeric[Y]].toDouble(y)
  }

  implicit object DoubleProduct extends DoubleProduct

  implicit object MonoidSeqDouble extends Monoid[Seq[Double]] {
    def combine(x: Seq[Double], y: Seq[Double]): Seq[Double] = x ++ y

    def zero: Seq[Double] = Nil
  }

  behavior of "Matrix2"

  it should "implement product by identity correctly (N=250)" taggedAs Slow in {
    given atMost: Duration = duration.FiniteDuration(1, "minute")

    productByIdentity(250)
  }

  // NOTE This runs just fine but it is really slow so normally, we ignore it.
  ignore should "implement product by identity correctly (N=500)" taggedAs Slow in {
    given atMost: Duration = duration.FiniteDuration(1, "minute")

    productByIdentity(500)
  }

  // NOTE This runs just fine but it is really slow so normally, we ignore it.
  ignore should "implement product by identity correctly (N=1000)" taggedAs Slow in {
    given atMost: Duration = duration.FiniteDuration(5, "minute")

    productByIdentity(1000)
  }

  private def productByIdentity(N: Int)(implicit atMost: Duration): Unit = Try {
    val r = Random
    val size = N
    val array = for (_ <- 1 to size) yield for (_ <- 1 to size) yield r.nextDouble()
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
  }.isSuccess shouldBe true

}



