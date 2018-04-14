package com.phasmid.majabigwaduce

/**
  * @tparam X the type which we want to create a monoid value for.
  *
  *           Created by scalaprof on 10/5/16.
  */
trait Monoid[X] extends Zero[X] {
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

  implicit object MonoidInt extends Zero.IntZero$ with Monoid[Int] {
    def combine(x: Int, y: Int): Int = x + y
  }

  implicit object MonoidString extends Zero.StringZero$ with Monoid[String] {
    def empty: String = ""

    def combine(x: String, y: String): String = x + y
  }

}

object Zero {

  trait DoubleZero$ extends Zero[Double] {
    def zero: Double = 0
  }

  implicit object DoubleZero$ extends DoubleZero$

  trait IntZero$ extends Zero[Int] {
    def zero: Int = 0
  }

  implicit object IntZero$ extends IntZero$


  trait StringZero$ extends Zero[String] {
    def zero: String = ""
  }

  implicit object StringZero$ extends StringZero$

}
