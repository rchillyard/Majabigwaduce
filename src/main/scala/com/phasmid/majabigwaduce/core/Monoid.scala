/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce.core

/**
 * @tparam X the type which we want to create a monoid value for.
 *
 *           Created by scalaprof on 10/5/16.
 */
trait Monoid[X] extends Zero[X]:
  /**
   * This is the "op" method of a Monoid, the one that associates any two instances of the monoid type.
   *
   * @param x the left-hand operand
   * @param y the right-hand operand
   * @return the result of combining x and y
   */
  def combine(x: X, y: X): X

/**
 * Type-class Zero is used to add behavior of initialization (or zeroing) of X.
 *
 * @tparam X the type which we want to create a zero value for.
 */
trait Zero[X]:
  /**
   * Method to create a zero/empty/nothing value of X
   *
   * @return an X which is zero (empty, etc.)
   */
  def zero: X

/**
 * Provides Monoid instances for standard types and utility methods for working with monoids.
 */
object Monoid:

  implicit object MonoidInt extends Zero.IntZero with Monoid[Int]:
    /**
     * Combines two integer values by performing addition.
     *
     * @param x the first integer value
     * @param y the second integer value
     * @return the result of adding x and y
     */
    def combine(x: Int, y: Int): Int = x + y

  /**
   * An implicit object representing a monoid instance for the Double type.
   * This instance provides the monoid operation (addition) and identity element (zero) for Double.
   */
  implicit object MonoidDouble extends Zero.DoubleZero with Monoid[Double]:
    /**
     * Combines two Double values by performing addition.
     *
     * @param x The first Double value.
     * @param y The second Double value.
     * @return The result of adding x and y.
     */
    def combine(x: Double, y: Double): Double = x + y

  /**
   * Provides an implicit instance of `Monoid` for the `String` type, with behavior
   * defined as string concatenation and an empty string as the identity element.
   *
   * This implementation extends `Zero.StringZero`, inheriting the `zero` value for `String`.
   */
  implicit object MonoidString extends Zero.StringZero with Monoid[String]:
    /**
     * Returns the identity element for the monoid operation on String.
     *
     * @return an empty String, which serves as the identity element.
     */
    def empty: String = ""

    /**
     * Combines two strings by concatenating them.
     *
     * @param x the first string to be combined
     * @param y the second string to be combined
     * @return a new string resulting from the concatenation of the input strings
     */
    def combine(x: String, y: String): String = x + y

  /**
   * Returns a `Monoid` instance for a tuple `(A, B)` where both `A` and `B` have `Monoid` instances.
   * The `combine` method for this tuple `Monoid` combines the elements of each tuple using the `combine`
   * method of their respective `Monoid` instances. The `zero` value represents a tuple where each element
   * is the `zero` value of the corresponding `Monoid`.
   *
   * @tparam A the type of the first element in the tuple, which has a `Monoid` instance
   * @tparam B the type of the second element in the tuple, which has a `Monoid` instance
   * @return a `Monoid` instance for the tuple `(A, B)`
   */
  implicit def monoidTuple[A: Monoid, B: Monoid]: Monoid[(A, B)] =
    new Monoid[(A, B)] {
      def combine(x: (A, B), y: (A, B)): (A, B) =
        val (xa, xb) = x
        val (ya, yb) = y
        (summon[Monoid[A]].combine(xa, ya), summon[Monoid[B]].combine(xb, yb))

      def zero: (A, B) = Zero.zeroTuple[A, B].zero
    }

  /**
   * Folds a sequence of elements of type `X` into a single value using the provided `Monoid` instance.
   * The folding operation starts with the `zero` value of the `Monoid` and combines all elements
   * of the sequence using the `combine` method of the `Monoid`.
   *
   * @param xs the sequence of elements to fold
   * @return the result of folding the sequence using the `Monoid`
   */
  def foldLeft[X: Monoid](xs: Seq[X]): X =
    val xm = summon[Monoid[X]]
    xs.foldLeft(xm.zero)(xm.combine)

object Zero {

  /**
   * A specialized implementation of the `Zero` type class for `Double`.
   * This trait provides a zero value for the `Double` type.
   */
  trait DoubleZero extends Zero[Double]:
    def zero: Double = 0

  implicit object DoubleZero extends DoubleZero

  trait IntZero extends Zero[Int]:
    def zero: Int = 0

  implicit object IntZero extends IntZero


  /**
   * A specialized implementation of the `Zero` type class for `String`.
   * This trait provides a zero (empty) value for strings.
   */
  trait StringZero extends Zero[String]:
    def zero: String = ""

  implicit object StringZero extends StringZero

  /**
   * A specialized implementation of the `Zero` type class for `Seq[X]`.
   * This trait provides a zero (empty) value for sequences of type `X`.
   *
   * @tparam X The element type of the sequence for which the zero (empty)
   *           value is defined.
   */
  trait SeqZero[X] extends Zero[Seq[X]]:
    def zero: Seq[X] = Nil

  implicit object IntSeqZero extends SeqZero[Int]

  /**
   * A specialized implementation of the `Zero` type class for `Map[Int, X]`.
   * This trait provides a zero (empty) value for a map with key type `Int` and
   * generic value type `X`.
   *
   * @tparam X The value type of the map for which the zero (empty) instance is defined.
   */
  trait VectorZero[X] extends Zero[Map[Int, X]]:
    def zero: Map[Int, X] = Map.empty

  implicit object IntVectorZero extends VectorZero[Int]

  implicit def zeroTuple[A: Zero, B: Zero]: Zero[(A, B)] = new Zero[(A, B)] {
    def zero: (A, B) = (summon[Zero[A]].zero, summon[Zero[B]].zero)
  }
}
