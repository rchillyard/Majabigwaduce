/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce.matrix

import com.phasmid.majabigwaduce.core.Monoid
import com.phasmid.majabigwaduce.dd.DataDefinition
import com.phasmid.majabigwaduce.dd.DataDefinition.*
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, duration}

/**
  * The Matrix[X] trait represents a sequence of X.
  *
  * @tparam X the underlying type of each row of the matrix (may itself be a sequence).
  */
trait Matrix[X]:
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
    build(forRows(productSpecial(_, ys))(summon[Monoid[Z]], ev, atMost, cutoff))

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

/**
  * Abstract class to implement some common methods of Matrix
  *
  * @tparam X the underlying type of each row of the matrix (may itself be a sequence).
  */
abstract class BaseMatrix[X] extends Matrix[X]:

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
    if size < cutoff
    then for t <- rows yield g(t)
    else
      Matrix.logger.info("forRows.1")
      val dd: DataDefinition[Int, X] = DataDefinition(for (t <- rows.zipWithIndex) yield t.swap)
      Matrix.logger.info("forRows.2")
      // Using map-reduce, apply the function g to each element of dd.
      val z: Map[Int, Z] = Await.result(dd.map(tupleLift(g)).apply(), atMost)
      Matrix.logger.info("forRows.3")
      // CONSIDER doing this more efficiently?
      for i <- 1 to size.rows yield z(i - 1)

/**
  * Case class to represent a one-dimensional matrix.
  *
  * @param rows   the elements of type T
  * @param atMost duration of total MapReduce time
  * @tparam T the underlying type of this matrix
  */
case class Matrix1[T: Numeric](rows: Seq[T])(implicit atMost: Duration) extends BaseMatrix[T]:

  protected def build[U: Numeric](us: Seq[U]): Matrix[U] = Matrix1(us)

  protected def productSpecial[Y: Numeric, Z: Product : Monoid](x: T, ys: Seq[Y]): Z =
    val zp = summon[Product[Z]]
    ys match
      case y :: Nil => zp.product(x, y)
      case _ => throw IncompatibleDimensionsException(1, ys.length)

/**
  * Case class to represent a two-dimensional matrix.
  *
  * @param rows the rows
  * @tparam T the underlying type of this matrix
  */
case class Matrix2[T: Numeric](rows: Seq[Seq[T]]) extends BaseMatrix[Seq[T]]:

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
  def product2[Y: Numeric, Z: Product : Monoid : Numeric](other: Matrix2[Y])(implicit ev: Monoid[Seq[T]], atMost: Duration, cutoff: Dimensions): Matrix[Seq[Z]] =
    implicit object MonoidSeqZ extends Monoid[Seq[Z]]:
      def combine(x: Seq[Z], y: Seq[Z]): Seq[Z] = x ++ y

      def zero: Seq[Z] = Nil

    if c == other.r
    then Matrix2(forRows(ts => for us <- other.cols yield productSpecial(ts, us))(summon[Monoid[Seq[Z]]], ev, atMost, cutoff))
    else throw IncompatibleDimensionsException(c, other.rows.length)

  private def cols: Seq[Seq[T]] = rows.transpose

  import Matrix2.given

  /**
   * Builds a new `Matrix` from the provided sequence of elements.
   *
   * @param us the sequence of elements, where each element represents a row of the resulting Matrix.
   *           The elements must have a type that has an instance of the `Numeric` type class.
   *
   * @tparam U the type of elements in the provided sequence, constrained to have a `Numeric` type class instance.
   * @return a `Matrix[U]` constructed using the provided sequence.
   */
  protected def build[U: Numeric](us: Seq[U]): Matrix[U] = Matrix1(us)

  /**
   * Computes a special product of two sequences and returns the result as an instance of type `Z`.
   * The sequences must have the same length for the computation to proceed.
   *
   * @param ts a sequence of elements of type `T`. Represents the first sequence in the computation.
   * @param ys a sequence of elements of type `Y`. Represents the second sequence in the computation.
   * @tparam Y the type of elements in the second sequence, which must have a `Numeric` type class instance.
   * @tparam Z the result type, which must have `Product` and `Monoid` type class instances.
   * @return the product of the two sequences combined using the `Product` operations and folded via the `Monoid`.
   * @throws IncompatibleDimensionsException if the provided sequences `ts` and `ys` have different lengths.
   */
  protected def productSpecial[Y: Numeric, Z: Product : Monoid](ts: Seq[T], ys: Seq[Y]): Z =
    if ts.length == ys.length
    then
      val zp = summon[Product[Z]]
      val vs: Seq[Z] = for (t, y) <- ts zip ys yield zp.product(t, y)
      Monoid.foldLeft(vs)
    else throw IncompatibleDimensionsException(ts.length, ys.length)

  private val r = rows.length
  private val c = rows.headOption.map(_.length).getOrElse(0) // CONSIDER checking other rows too

/**
  * Type-class trait which knows how to multiply an X and a Y to yield a Z
  *
  * @tparam Z the result type
 */
trait Product[Z]:
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

/**
  * Case class to represent the dimensions of a matrix.
  * Currently, area works only for one- or two-dimensional matrices
  *
  * @param xs a sequence of integers defining the numbers of rows, columns, etc.
 */
case class Dimensions(xs: Seq[Int]) extends Ordered[Dimensions]:
  /**
   * Returns the number of elements in the collection.
   *
   * @return the size of the collection as an integer
   */
  def size: Int = xs.length

  /**
   * Retrieves the number of rows in the collection by inspecting the first element.
   * If the collection is empty, it defaults to 0.
   *
   * @return The number of rows as an integer.
   */
  def rows: Int = xs.headOption.getOrElse(0)

  /**
   * Represents the number of columns in the matrix.
   * Defaults to 1 if the matrix has no second dimension.
   */
  val cols: Int = xs.tail.headOption.getOrElse(1)

  /**
   * Calculates the area of a two-dimensional matrix based on its dimensions.
   * The area is computed as the product of the number of rows and columns.
   * If rows or columns are not defined, default values are used (0 for rows, and 1 for columns).
   *
   * @return the computed area as an integer value
   */
  def area: Int = rows * cols

  def compare(that: Dimensions): Int = Ordering[Int].compare(area, that.area)

/**
 * Object Matrix serves as a utility and companion object for operations related to matrices.
 * It provides various matrix-specific methods such as Kronecker Delta
 * and utility definitions including logging.
 */
object Matrix:
  /**
    * The Kronecker Delta function.
    * CONSIDER is this a performance bottleneck?
    *
    * @return 1 if i==j otherwise 0
   */
  def kroneckerDelta[T: Numeric](i: Int, j: Int): T =
    if i == j
    then Numeric[T].fromInt(1)
    else Numeric[T].zero

  val logger: Logger = LoggerFactory.getLogger(Matrix.getClass)

/**
 * Object Matrix1 provides an implicit duration parameter `atMost`.
 * The `atMost` parameter defines a finite timeout duration of 1 second.
 */
object Matrix1:
  given atMost: Duration = duration.FiniteDuration(1, "second")

/**
 * Companion object for the `Matrix2` class, offering utility methods and definitions
 * related to matrix creation and operations, as well as given values for configuration purposes.
 */
object Matrix2 {
  given atMost: Duration = duration.FiniteDuration(10, "second")

  given cutoff: Dimensions = Dimensions(Seq(20, 20))

  /**
    * Method to create an identity matrix of order n.
    *
    * @param n the required size of the rows and columns.
    * @tparam T the underlying type.
    * @return a matrix of size n x n with 1s down the diagonal and zeros elsewhere.
    */
  def identity[T: Numeric](n: Int): Matrix2[T] = Matrix2[T](for (i <- 0 until n) yield for (j <- 0 until n) yield Matrix.kroneckerDelta(i, j))
}

/**
 * Base class for exceptions that can occur during matrix operations.
 *
 * This exception serves as a parent class for more specific exceptions
 * related to matrix computations, such as dimension or length mismatches.
 *
 * @param str The detailed error message providing contextual information
 *            about the nature of the exception.
 */
abstract class MatrixException(str: String) extends Exception(str, null)

/**
 * Exception thrown to indicate that two matrices have incompatible dimensions
 * for the operation being performed.
 *
 * This exception generally occurs when the number of columns in the left-hand
 * side (LHS) matrix does not match the number of rows in the right-hand side
 * (RHS) matrix, a requirement for operations like matrix multiplication.
 *
 * @param cols The number of columns in the LHS matrix.
 * @param rows The number of rows in the RHS matrix.
 * @throws IncompatibleDimensionsException when matrix dimensions are not suitable
 *                                         for the intended operation.
 */
case class IncompatibleDimensionsException(cols: Int, rows: Int) extends MatrixException(s"# columns of LHS ($cols)" +
  s"does not match # rows of RHS ($rows)")

/**
 * Exception that indicates a mismatch between two lengths during matrix operations.
 *
 * This exception is typically thrown when two sequences or matrices, each with a specific length, are
 * expected to align but do not. For example, when performing element-wise operations on two vectors,
 * their lengths must match; otherwise, this exception is raised.
 *
 * @param l1 The first length involved in the operation.
 * @param l2 The second length involved in the operation.
 */
case class IncompatibleLengthsException(l1: Int, l2: Int) extends MatrixException(s"length $l1 does not match length $l2")

/**
 * Companion object for the `Dimensions` class, providing utility methods
 * and predefined instances for working with matrix dimensions.
 */
object Dimensions:
  given cutoff: Dimensions = Dimensions(Seq(20, 20))

  /**
   * Creates a `Dimensions` instance from the provided integer arguments.
   *
   * @param xs a variable number of integers representing the dimensions (e.g., rows, columns, etc.).
   * @return a `Dimensions` object initialized with the provided values.
   */
  def create(xs: Int*): Dimensions = apply(xs)
