package com.phasmid.majabigwaduce.core

/**
 * A trait that defines a lazy monad, which wraps an Iterable and provides
 * functionality for applying transformations and filters in a lazy manner.
 *
 * @tparam T the type of elements in the wrapped iterable before transformation.
 * @tparam U the type of elements in the wrapped iterable after transformation.
 */
trait LazyMonad[T, U] extends Iterable[U]:

  /**
   * An iterable which forms the (initial) wrapped value of this lazy monad.
   *
   * @return an Iterable[U]
   */
  val ts: Iterable[T]

  /**
   * A function with which to map the wrapped value of this lazy monad in order to form an iterator.
   */
  val f: T => U

  /**
   * A predicate which is a pre-filter (applies to the wrapped values, before being mapped with f).
   */
  val p: T => Boolean

  /**
   * The method used to construct a concrete result after applying map or one of the filter methods.
   *
   * @param iterable the Iterable[A] which is to be wrapped.
   * @param g        a function A => B.
   * @param p        a predicate on A.
   * @tparam A the underlying type of the wrapped iterable.
   * @tparam B the underlying type of the iterator.
   * @return a LazyMonad[A, B].
   */
  protected def build[A, B](iterable: Iterable[A])(g: A => B, p: A => Boolean): LazyMonad[A, B]

  /**
   * The map method.
   *
   * @param g the function U => V which will be applied to the values of the wrapped iterable.
   * @tparam V the underlying iterator type of the resulting LazyMonad.
   * @return a LazyMonad[T, V]
   */
  override def map[V](g: U => V): LazyMonad[T, V] =
    build(ts)(f andThen g, p)

  /**
   * Method to pre-filter the wrapped iterable.
   *
   * @param q a predicate of type T => Boolean
   * @return a new LazyMonad[T, U].
   */
  def preFilter(q: T => Boolean): LazyMonad[T, U] =
    build(ts)(f, t => p(t) && q(t))

  /**
   * Method defined by Iterable[U].
   *
   * @param q a predicate of type U => Boolean
   * @return a new LazyMonad[T, U] in the form of an Iterable[U].
   */
  override def filter(q: U => Boolean): Iterable[U] =
    build(ts)(f, f andThen q)

  /**
   * Method defined by Iterable[U] which eagerly evaluates this LazySequence.
   *
   * @return an Iterator[U] formed from pre-filtering the wrapped iterable by p and then mapping what's left with f.
   */
  def iterator: Iterator[U] = applyFilterAndMap.iterator

  /**
   * Applies a filter and a mapping operation to the wrapped iterable.
   * The filter operation removes elements of the iterable that do not satisfy a given predicate.
   * The mapping operation applies a transformation function to the remaining elements.
   *
   * @return An Iterable[U] containing elements that satisfy the predicate and have been transformed by the mapping function.
   */
  private def applyFilterAndMap: Iterable[U] =
    ts.filter(p).map(f)

/**
 * A case class that extends LazyMonad and provides a mechanism to lazily transform and filter an iterable collection.
 * The transformation and filtering are performed using a mapping function and a predicate respectively.
 *
 * @param ts the initial iterable collection of type T.
 * @param f  the mapping function T => U to transform elements of the collection.
 * @param p  the predicate T => Boolean to pre-filter elements in the collection before applying the mapping function.
 * @tparam T the type of the elements in the initial collection.
 * @tparam U the type of the elements in the resulting collection after applying the mapping function.
 */
case class LazySequence[T, U](ts: Iterable[T], f: T => U, p: T => Boolean) extends LazyMonad[T, U]:

  /**
   * Non-instance method to construct a new LazySequence based on the parameters: iterable, g, and q.
   *
   * @param iterable the Iterable[A] which is to be wrapped.
   * @param g        a function A => B.
   * @param q        a predicate on A.
   * @tparam A the underlying type of the wrapped iterable.
   * @tparam B the underlying type of the iterator.
   * @return a LazyMonad[A, B].
   */
  def build[A, B](iterable: Iterable[A])(g: A => B, q: A => Boolean): LazyMonad[A, B] =
    LazySequence(iterable, g, q)

/**
 * Companion object for the LazyMonad type.
 * Provides factory methods to create instances of LazySequence, which is a concrete implementation of LazyMonad.
 */
object LazyMonad:
  /**
   * Method to construct a new LazySequence from an iterable with, effectively, no filter nor predicate.
   *
   * @param ts the iterable to be wrapped.
   * @param f  a function to turn a T into a U.
   * @param p  A predicate which is a pre-filter (applies to the wrapped values, before being mapped with f).
   * @tparam T the underlying type of ts.
   * @tparam U the underlying type of the resulting Iterable.
   * @return a new LazySequence based on ts.
   */
  def apply[T, U](ts: Iterable[T], f: T => U, p: T => Boolean): LazyMonad[T, U] =
    new LazySequence[T, U](ts, f, p)

  /**
   * Method to construct a new LazySequence from an iterable with, effectively, no filter nor predicate.
   *
   * @param ts the iterable to be wrapped.
   * @tparam T the underlying type of ts.
   * @return a new LazySequence based on ts.
   */
  def apply[T](ts: Iterable[T]): LazyMonad[T, T] = apply(ts, identity)

  /**
   * Method to construct a new LazySequence from an iterable with a function but no filtration.
   *
   * @param ts the iterable to be wrapped.
   * @param f  a function to turn a T into a U.
   * @tparam T the underlying type of ts.
   * @tparam U the underlying type of the resulting Iterable.
   * @return a LazyMonad[T, U] based on ts and f (with no filtration).
   */
  def apply[T, U](ts: Iterable[T], f: T => U): LazyMonad[T, U] =
    LazySequence(ts, f, _ => true)

  /**
   * Method to construct a new LazySequence from an iterable with a filter but no function.
   *
   * @param ts the iterable to be wrapped.
   * @param p  A predicate which is a pre-filter (applies to the wrapped values, before being mapped with f).
   * @tparam T the underlying type of ts.
   * @return a new LazySequence[T, T]
   */
  def withFilter[T](ts: Iterable[T], p: T => Boolean): LazyMonad[T, T] =
    LazySequence[T, T](ts, identity, p)
