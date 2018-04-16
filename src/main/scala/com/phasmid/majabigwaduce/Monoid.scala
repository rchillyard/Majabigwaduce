package com.phasmid.majabigwaduce

/**
  * @tparam X the type which we want to create a monoid value for.
  *
  *           Created by scalaprof on 10/5/16.
  */
trait Monoid[X] extends Zero[X] {
  /**
    * This is the "op" method of a Monoid, the one that associates any two instances of the monoid type.
    *
    * @param x the left-hand operand
    * @param y the right-hand operand
    * @return the result of combining x and y
    */
  def combine(x: X, y: X): X
}

/**
  * Type-class Zero is used to add behavior of initialization (or zeroing) of X.
  *
  * @tparam X the type which we want to create a zero value for.
  */
trait Zero[X] {
  /**
    * Method to create a zero/empty/nothing value of X
    *
    * @return an X which is zero (empty, etc.)
    */
  def zero: X
}

object Monoid {

  implicit object MonoidInt extends Zero.IntZero with Monoid[Int] {
    def combine(x: Int, y: Int): Int = x + y
  }

  implicit object MonoidString extends Zero.StringZero with Monoid[String] {
    def empty: String = ""

    def combine(x: String, y: String): String = x + y
  }

  def foldLeft[X: Monoid](xs: Seq[X]): X = {
    val xm = implicitly[Monoid[X]]
    xs.foldLeft(xm.zero)(xm.combine)
  }
}

object Zero {

  trait DoubleZero extends Zero[Double] {
    def zero: Double = 0
  }

  implicit object DoubleZero extends DoubleZero

  trait IntZero extends Zero[Int] {
    def zero: Int = 0
  }

  implicit object IntZero extends IntZero


  trait StringZero extends Zero[String] {
    def zero: String = ""
  }

  implicit object StringZero extends StringZero

  trait SeqZero[X] extends Zero[Seq[X]] {
    def zero: Seq[X] = Nil
  }

  implicit object IntSeqZero extends SeqZero[Int]

}
