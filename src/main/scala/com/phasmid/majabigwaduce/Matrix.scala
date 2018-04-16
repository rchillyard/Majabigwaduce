package com.phasmid.majabigwaduce

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
    * @return a sequence of integers which has length of one element at least.
    */
  def size: Seq[Int]

  /**
    * Determine the product of this Matrix with m
    *
    * @param m      the multiplicand
    * @param ev     evidence of Monoid[X]
    * @param atMost duration of total MapReduce time
    * @tparam Y the underlying type of m
    * @tparam Z the underlying type of the result
    * @return a Matrix which is the product of this and m
    */
  def product[Y: Numeric, Z: Product : Monoid : Numeric](m: Matrix[Y])(implicit ev: Monoid[X], atMost: Duration): Matrix[Z] =
    product(m.rows)

  /**
    * Determine the product of this Matrix with a sequence of Y objects
    *
    * @param ys     the rows of Y to be multiplied with this Matrix
    * @param ev     evidence of Monoid[X]
    * @param atMost duration of total MapReduce time
    * @tparam Y the type of the multiplicand rows
    * @tparam Z the underlying type of the result
    * @return a Matrix which is the product of this and m
    */
  def product[Y: Numeric, Z: Product : Monoid : Numeric](ys: Seq[Y])(implicit ev: Monoid[X], atMost: Duration): Matrix[Z] =
    build(forRows(f(_, ys))(implicitly[Monoid[Z]], ev, atMost))

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
    * If the number of rows is less than the cutoff, then we do the operation in the current thread;
    * otherwise we do it using MapReduce.
    *
    * @param g      a function which takes an X and yields a Z
    * @param ev     evidence of Monoid[X]
    * @param atMost duration of total MapReduce time
    * @tparam Z the underlying type of the result
    * @return a Matrix which is the product of this and m
    */
  protected def forRows[Z: Monoid](g: X => Z)(implicit ev: Monoid[X], atMost: Duration): Seq[Z]

  /**
    * Method to pair an X and a Seq[Y] to yield a Z.
    * If X is a Seq[T] then f yields the dot product.
    * If X is a Seq[Seq[T], then f yields a sequence of dot products.
    *
    * @param x  the X value
    * @param ys the Seq[Y] value
    * @tparam Y the underlying type of ys
    * @tparam Z the type of the result
    * @return the result
    */
  protected def f[Y: Numeric, Z: Product : Monoid](x: X, ys: Seq[Y]): Z
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
  def size = Seq(rows.length)

  protected def forRows[Z: Monoid](g: X => Z)(implicit ev: Monoid[X], atMost: Duration): Seq[Z] =
    if (rows.length < Matrix.cutoff) for (t <- rows) yield g(t)
    else {
      import scala.language.postfixOps
      val dd = DataDefinition.apply((for (tuple <- rows zipWithIndex) yield tuple.swap).toMap)
      val z = Await.result(dd.map(g).apply(), atMost)
      // CONSIDER doing this more efficiently?
      for (i <- 1 to size.head) yield z(i - 1)
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

  protected def f[Y: Numeric, Z: Product : Monoid](x: T, ys: Seq[Y]): Z = {
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
  * @param rows   the rows
  * @param atMost duration of total MapReduce time
  * @tparam T the underlying type of this matrix
  */
case class Matrix2[T: Numeric](rows: Seq[Seq[T]])(implicit atMost: Duration) extends BaseMatrix[Seq[T]] {

  override def size = Seq(r, c)

  def transpose: Seq[Seq[T]] = cols

  def product2[Y: Numeric, Z: Product : Monoid : Numeric](other: Matrix2[Y])(implicit ev: Monoid[Seq[T]], atMost: Duration): Matrix[Seq[Z]] = {
    implicit object MonoidSeqZ extends Monoid[Seq[Z]] {
      def combine(x: Seq[Z], y: Seq[Z]): Seq[Z] = x ++ y

      def zero: Seq[Z] = Nil
    }
    if (c == other.r) Matrix2(forRows(ts => for (us <- other.cols) yield f(ts, us))(implicitly[Monoid[Seq[Z]]], ev, atMost))
    else throw IncompatibleDimensionsException(c, other.rows.length)
  }

  private def cols: Seq[Seq[T]] = rows.transpose

  protected def build[U: Numeric](us: Seq[U]): Matrix[U] = Matrix1(us)

  protected def f[Y: Numeric, Z: Product : Monoid](ts: Seq[T], ys: Seq[Y]): Z = if (ts.length == ys.length) {
    val zp = implicitly[Product[Z]]
    val vs: Seq[Z] = for ((t, y) <- ts zip ys) yield zp.product(t, y)
    Monoid.foldLeft(vs)
  }
  else throw IncompatibleDimensionsException(ts.length, ys.length)

  private val r = rows.length
  private val c = rows.head.length // CONSIDER checking other rows too
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

object Matrix {
  /**
    * The Kronecker Delta function.
    * CONSIDER is this a performance bottleneck?
    *
    * @return 1 if i==j otherwise 0
    */
  def kroneckerDelta[T: Numeric](i: Int, j: Int): T = if (i == j) implicitly[Numeric[T]].fromInt(1) else implicitly[Numeric[T]].zero

  // TODO make this an implicit parameter in methods
  var cutoff = 1000
}

object Matrix1 {
  implicit val atMost: Duration = duration.FiniteDuration(1, "second")
}

object Matrix2 {
  implicit val atMost: Duration = duration.FiniteDuration(10, "second")

  def identity[T: Numeric](n: Int): Matrix2[T] = Matrix2[T](for (i <- 0 until n) yield for (j <- 0 until n) yield Matrix.kroneckerDelta(i,j))
}

abstract class MatrixException(str: String) extends Exception(str, null)

case class IncompatibleDimensionsException(cols: Int, rows: Int) extends MatrixException(s"# columns of LHS ($cols)" +
  s"does not match # rows of RHS ($rows)")
