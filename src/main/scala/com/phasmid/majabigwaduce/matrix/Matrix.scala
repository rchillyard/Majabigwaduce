/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce.matrix

import com.phasmid.majabigwaduce.core.Monoid
import com.phasmid.majabigwaduce.dd.DataDefinition
import com.phasmid.majabigwaduce.dd.DataDefinition._
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, duration}

/**
  * The Matrix[X] trait represents a sequence of X.
  *
  * @tparam X the underlying type of each row of the matrix (may itself be a sequence).
  */
trait Matrix[X] {
  /**
    * Determine the numbers of rows, columns, etc.
    *
    * @return a Dimensions object (a sequence of integers which has length of one element at least).
    */
  def size: Dimensions

  /**
    * Determine the product of this Matrix with m
    *
    * @param m      the multiplicand
    * @param ev     evidence of Monoid[X]
    * @param atMost duration of total MapReduce time
    * @param cutoff the maximum size we should implement in current thread
    * @tparam Y the underlying type of m
    * @tparam Z the underlying type of the result
    * @return a Matrix which is the product of this and m
    */
  def product[Y: Numeric, Z: Product : Monoid : Numeric](m: Matrix[Y])(implicit ev: Monoid[X], atMost: Duration, cutoff: Dimensions): Matrix[Z] =
    product(m.rows)

  /**
    * Determine the product of this Matrix with a sequence of Y objects
    *
    * @param ys     the rows of Y to be multiplied with this Matrix
    * @param ev     evidence of Monoid[X]
    * @param atMost duration of total MapReduce time
    * @param cutoff the maximum size we should implement in current thread
    * @tparam Y the type of the multiplicand rows
    * @tparam Z the underlying type of the result
    * @return a Matrix which is the product of this and m
    */
  def product[Y: Numeric, Z: Product : Monoid : Numeric](ys: Seq[Y])(implicit ev: Monoid[X], atMost: Duration, cutoff: Dimensions): Matrix[Z] =
    build(forRows(productSpecial(_, ys))(implicitly[Monoid[Z]], ev, atMost, cutoff))

  /**
    * The rows of this Matrix
    *
    * @return
    */
  def rows: Seq[X]

  /**
    * Method to build a Matrix from a sequence of Z objects
    *
    * @param zs the sequence of rows to be used for the resulting Matrix
    * @tparam Z the underlying type of the result
    * @return a Matrix[Z] whose rows are made up of zs
    */
  protected def build[Z: Numeric](zs: Seq[Z]): Matrix[Z]

  /**
    * Method to process the rows of this Matrix.
    * If the size is less than the cutoff, then we do the operation in the current thread;
    * otherwise we do it using MapReduce.
    *
    * @param g      a function which takes an X and yields a Z
    * @param ev     evidence of Monoid[X]
    * @param atMost duration of total MapReduce time
    * @param cutoff the maximum size we should implement in current thread
    * @tparam Z the underlying type of the result
    * @return a Matrix which is the product of this and m
    */
  protected def forRows[Z: Monoid](g: X => Z)(implicit ev: Monoid[X], atMost: Duration, cutoff: Dimensions): Seq[Z]

  /**
    * Method to pair an X and a Seq[Y] to yield a Z.
    * If X is a Seq[T] then productSpecial yields the dot product.
    * If X is a Seq[Seq[T], then productSpecial yields a sequence of dot products.
    *
    * NOTE: this method is an instance method (defined in Matrix trait).
    * CONSIDER defining the two forms of this method in Matrix1 and Matrix2 companion objects.
    *
    * @param x  the X value
    * @param ys the Seq[Y] value
    * @tparam Y the underlying type of ys
    * @tparam Z the type of the result
    * @return the result
    */
  protected def productSpecial[Y: Numeric, Z: Product : Monoid](x: X, ys: Seq[Y]): Z
}

/**
  * Abstract class to implement some common methods of Matrix
  *
  * @tparam X the underlying type of each row of the matrix (may itself be a sequence).
  */
abstract class BaseMatrix[X] extends Matrix[X] {

  /**
    * The number of rows.
    * This method may be overridden
    *
    * @return a sequence of one integer.
    */
  def size: Dimensions = Dimensions.create(rows.length)

  /**
    * Method to process the rows of this Matrix.
    * If the size is less than the cutoff, then we do the operation in the current thread;
    * otherwise we do it using MapReduce.
    *
    * @param g      a function which takes an X and yields a Z
    * @param ev     evidence of Monoid[X]
    * @param atMost duration of total MapReduce time
    * @param cutoff the maximum size we should implement in current thread
    * @tparam Z the underlying type of the result
    * @return a Matrix which is the product of this and m
    */
  protected def forRows[Z: Monoid](g: X => Z)(implicit ev: Monoid[X], atMost: Duration, cutoff: Dimensions): Seq[Z] =
    if (size < cutoff) for (t <- rows) yield g(t)
    else {
      Matrix.logger.info("forRows.1")
      val dd: DataDefinition[Int, X] = DataDefinition(for (t <- rows.zipWithIndex) yield t.swap)
      Matrix.logger.info("forRows.2")
      // Using map-reduce, apply the function g to each element of dd.
      val z: Map[Int, Z] = Await.result(dd.map(tupleLift(g)).apply(), atMost)
      Matrix.logger.info("forRows.3")
      // CONSIDER doing this more efficiently?
      for (i <- 1 to size.rows) yield z(i - 1)
    }
}

/**
  * Case class to represent a one-dimensional matrix.
  *
  * @param rows   the elements of type T
  * @param atMost duration of total MapReduce time
  * @tparam T the underlying type of this matrix
  */
case class Matrix1[T: Numeric](rows: Seq[T])(implicit atMost: Duration) extends BaseMatrix[T] {

  protected def build[U: Numeric](us: Seq[U]): Matrix[U] = Matrix1(us)

  protected def productSpecial[Y: Numeric, Z: Product : Monoid](x: T, ys: Seq[Y]): Z = {
    val zp = implicitly[Product[Z]]
    ys match {
      case y :: Nil => zp.product(x, y)
      case _ => throw IncompatibleDimensionsException(1, ys.length)
    }
  }
}

/**
  * Case class to represent a two-dimensional matrix.
  *
  * @param rows the rows
  * @tparam T the underlying type of this matrix
  */
case class Matrix2[T: Numeric](rows: Seq[Seq[T]]) extends BaseMatrix[Seq[T]] {

  override def size: Dimensions = Dimensions.create(r, c)

  def transpose: Seq[Seq[T]] = cols

  /**
    * Method to multiply this Matrix2 with another Matrix2.
    *
    * @param other  the other Matrix2.
    * @param ev     evidence that Seq[T] is a monoid.
    * @param atMost max duration.
    * @param cutoff the cutoff (see Dimensions).
    * @tparam Y the underlying type of the other Matrix2.
    * @tparam Z the underlying type of the resulting Matrix.
    * @return the product of this and other as a Matrix
    */
  def product2[Y: Numeric, Z: Product : Monoid : Numeric](other: Matrix2[Y])(implicit ev: Monoid[Seq[T]], atMost: Duration, cutoff: Dimensions): Matrix[Seq[Z]] = {
    implicit object MonoidSeqZ extends Monoid[Seq[Z]] {
      def combine(x: Seq[Z], y: Seq[Z]): Seq[Z] = x ++ y

      def zero: Seq[Z] = Nil
    }
    if (c == other.r) Matrix2(forRows(ts => for (us <- other.cols) yield productSpecial(ts, us))(implicitly[Monoid[Seq[Z]]], ev, atMost, cutoff))
    else throw IncompatibleDimensionsException(c, other.rows.length)
  }

  private def cols: Seq[Seq[T]] = rows.transpose

  import Matrix2._

  protected def build[U: Numeric](us: Seq[U]): Matrix[U] = Matrix1(us)

  protected def productSpecial[Y: Numeric, Z: Product : Monoid](ts: Seq[T], ys: Seq[Y]): Z = if (ts.length == ys.length) {
    val zp = implicitly[Product[Z]]
    val vs: Seq[Z] = for ((t, y) <- ts zip ys) yield zp.product(t, y)
    Monoid.foldLeft(vs)
  }
  else throw IncompatibleDimensionsException(ts.length, ys.length)

  private val r = rows.length
  private val c = rows.headOption.map(_.length).getOrElse(0) // CONSIDER checking other rows too
}

/**
  * Type-class trait which knows how to multiply an X and a Y to yield a Z
  *
  * @tparam Z the result type
  */
trait Product[Z] {
  /**
    * Method to multiply x and y (order will normally be unimportant)
    *
    * @param x the first parameter
    * @param y the second parameter
    * @tparam X the type of x
    * @tparam Y the type of y
    * @return the product of x and y
    */
  def product[X: Numeric, Y: Numeric](x: X, y: Y): Z
}

/**
  * Case class to represent the dimensions of a matrix.
  * Currently, area works only for one- or two-dimensional matrices
  *
  * @param xs a sequence of integers defining the numbers of rows, columns, etc.
  */
case class Dimensions(xs: Seq[Int]) extends Ordered[Dimensions] {
  def size: Int = xs.length

  def rows: Int = xs.headOption.getOrElse(0)

  val cols: Int = xs.tail.headOption.getOrElse(1)

  def area: Int = rows * cols

  def compare(that: Dimensions): Int = implicitly[Ordering[Int]].compare(area, that.area)
}

object Matrix {
  /**
    * The Kronecker Delta function.
    * CONSIDER is this a performance bottleneck?
    *
    * @return 1 if i==j otherwise 0
    */
  def kroneckerDelta[T: Numeric](i: Int, j: Int): T = if (i == j) implicitly[Numeric[T]].fromInt(1) else implicitly[Numeric[T]].zero

  val logger: Logger = LoggerFactory.getLogger(Matrix.getClass)
}

object Matrix1 {
  implicit val atMost: Duration = duration.FiniteDuration(1, "second")
}

object Matrix2 {
  implicit val atMost: Duration = duration.FiniteDuration(10, "second")

  implicit val cutoff: Dimensions = Dimensions(Seq(20, 20))

  /**
    * Method to create an identity matrix of order n.
    *
    * @param n the required size of the rows and columns.
    * @tparam T the underlying type.
    * @return a matrix of size n x n with 1s down the diagonal and zeros elsewhere.
    */
  def identity[T: Numeric](n: Int): Matrix2[T] = Matrix2[T](for (i <- 0 until n) yield for (j <- 0 until n) yield Matrix.kroneckerDelta(i, j))
}

abstract class MatrixException(str: String) extends Exception(str, null)

case class IncompatibleDimensionsException(cols: Int, rows: Int) extends MatrixException(s"# columns of LHS ($cols)" +
  s"does not match # rows of RHS ($rows)")

case class IncompatibleLengthsException(l1: Int, l2: Int) extends MatrixException(s"length $l1 does not match length $l2")

object Dimensions {
  implicit val cutoff: Dimensions = Dimensions(Seq(20, 20))

  def create(xs: Int*): Dimensions = apply(xs)
}