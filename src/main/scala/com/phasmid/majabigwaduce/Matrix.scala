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

  def product[Y: Numeric, Z: Product : Monoid : Numeric](us: Seq[Y])(implicit ev: Monoid[X], atMost: Duration): Matrix[Z] =
    build(forRows(f(_, us))(implicitly[Monoid[Z]], ev, atMost))

  def rows: Seq[X]

  def build[Z: Numeric](tss: Seq[Z]): Matrix[Z]

  protected def forRows[Y, Z: Monoid](g: X => Z)(implicit ev: Monoid[X], atMost: Duration): Seq[Z]

  protected def g[Y: Numeric, Z: Product : Monoid](us: Seq[Y]): X => Z

  protected def f[Y: Numeric, Z: Product : Monoid](x: X, ys: Seq[Y]): Z
}

abstract class BaseMatrix[X] extends Matrix[X] {

  def size = Seq(rows.length)

  protected def forRows[Y, Z: Monoid](g: X => Z)(implicit ev: Monoid[X], atMost: Duration): Seq[Z] =
    if (rows.length < Matrix.cutoff) for (t <- rows) yield g(t)
    else {
      import scala.language.postfixOps
      val dd = DataDefinition.apply((for (tuple <- rows zipWithIndex) yield tuple.swap).toMap)
      val z = Await.result(dd.map(g).apply(), atMost)
      // CONSIDER doing this more efficiently?
      for (i <- 1 to size.head) yield z(i-1)
    }
}

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

  def build[U: Numeric](us: Seq[U]): Matrix[U] = Matrix1(us)

  protected def g[Y: Numeric, Z: Product : Monoid](us: Seq[Y]): Seq[T] => Z = { ts: Seq[T] => f(ts, us) }

  protected def f[Y: Numeric, Z: Product : Monoid](ts: Seq[T], ys: Seq[Y]): Z = if (ts.length == ys.length) {
    val zp = implicitly[Product[Z]]
    val vs: Seq[Z] = for ((t, y) <- ts zip ys) yield zp.product(t, y)
    Monoid.foldLeft(vs)
  }
  else throw IncompatibleDimensionsException(ts.length, ys.length)

  private val r = rows.length
  private val c = rows.head.length // CONSIDER checking other rows too
}

case class Matrix1[T: Numeric](rows: Seq[T])(implicit atMost: Duration) extends BaseMatrix[T] {

  def build[U: Numeric](us: Seq[U]): Matrix[U] = Matrix1(us)

  protected def g[Y: Numeric, Z: Product : Monoid](us: Seq[Y]): T => Z = { t: T => f(t, us) }

  protected def f[Y: Numeric, Z: Product : Monoid](x: T, ys: Seq[Y]): Z = {
    val zp = implicitly[Product[Z]]
    ys match {
      case y :: Nil => zp.product(x, y)
      case _ => throw IncompatibleDimensionsException(1, ys.length)
    }
  }
}

trait Product[Z] {
  def product[X: Numeric, Y: Numeric](x: X, y: Y): Z
}

object Matrix {
  var cutoff = 1000
}

object Matrix1 {
  implicit val atMost: Duration = duration.FiniteDuration(1, "second")
}

object Matrix2 {
  implicit val atMost: Duration = duration.FiniteDuration(10, "second")
}

abstract class MatrixException(str: String) extends Exception(str, null)

case class IncompatibleDimensionsException(cols: Int, rows: Int) extends MatrixException(s"# columns of LHS ($cols)" +
  s"does not match # rows of RHS ($rows)")
