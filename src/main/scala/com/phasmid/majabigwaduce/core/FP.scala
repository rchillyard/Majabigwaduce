package com.phasmid.majabigwaduce.core

import com.phasmid.majabigwaduce.matrix.IncompatibleLengthsException

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
  * This module contains functional programming methods which can also be found in the FP module of LaScala
  */
object FP:

  /**
   * Lifts a function `f` of type `T => R` into a function of type `T => Try[R]`.
   * This allows the application of `f` to a value of type `T` while capturing
   * any exceptions that may be thrown during its execution as a `Failure`.
   * If the function executes successfully, the result is wrapped in a `Success`.
   *
   * @param f the function to be lifted, of type `T => R`.
   * @tparam T the input type of the function.
   * @tparam R the output type of the function.
   * @return a new function of type `T => Try[R]` that applies the original function
   *         and wraps its result in a `Try`.
   */
  def lift[T, R](f: T => R): T => Try[R] =
    t => Try(f(t))

  /**
   * Lifts a binary function `(T1, T2) => R` into a function 
   * that returns a `Try[R]`, capturing any exceptions that 
   * might occur during its execution.
   *
   * @param f the binary function to be lifted, of type `(T1, T2) => R`.
   * @tparam T1 the type of the first input parameter of the function.
   * @tparam T2 the type of the second input parameter of the function.
   * @tparam R  the return type of the function.
   * @return a new function of type `(T1, T2) => Try[R]` that applies 
   *         the original function and wraps its result in a `Try`.
   */
  def lift[T1, T2, R](f: (T1, T2) => R): (T1, T2) => Try[R] =
    (t1, t2) => Try(f(t1, t2))

  /**
    * Method flatten which takes a Future[Try[X]\] and returns a Future[X].
    *
    * @param xyf      a Future of Try of X.
    * @param executor (implicit) the ExecutionContext
    * @tparam X the underlying type.
    * @return a Future[X], which will be successful if the Try was a success, otherwise a failure.
    */
  def flatten[X](xyf: Future[Try[X]])(implicit executor: ExecutionContext): Future[X] =
    for
      xy <- xyf
      x <- asFuture(xy)
    yield x

  /**
    * Method to flatten a Try[Future[X]\] (not a common occurrence).
    *
    * @param xfy      a Try of Future of X.
    * @param executor (implicit) the ExecutionContext
    * @tparam X the underlying type.
    * @return a Future[X], which will be successful if the Try was a success, otherwise a failure.
    */
  def flatten[X](xfy: Try[Future[X]])(implicit executor: ExecutionContext): Future[X] = xfy match
    case Success(xf) => for (x <- xf) yield x
    case Failure(t) => Future.failed(t)

  /**
    * Method to take a Map[K,Either[X,V]\] and generated a tuple of two sequenced-maps, each of the same form as the input but containing only the left-values or right-values as appropriate.
    *
    * @param vXeKm the input map
    * @tparam K the key type
    * @tparam V the value type
    * @tparam X the partition type
    * @return a tuple of Map[K,Either[X,V]\] maps in sequenced form.
    * */
  def partition[K, V, X](vXeKm: Map[K, Either[X, V]]): (Seq[(K, Either[X, V])], Seq[(K, Either[X, V])]) =
    vXeKm.toSeq.partition({ case (_, v) => v.isLeft })

  /**
    * Method sequence which applied to a Try[X] returns an Either[Throwable,X].
    *
    * @param xt : Try[X]
    * @tparam X the underlying type
    * @return : Either[Throwable,X]
    */
  def toEither[X](xt: Try[X]): Either[Throwable, X] = xt match {
    case Success(s) => Right(s);
    case Failure(e) => Left(e)
  }

  /**
    * Method sequence which, applied to a Seq[Try[X]\], returns a Try[Seq[X]\]
    *
    * @param xts : Seq[Try[X]\]
    * @tparam X the underlying type
    * @return : Try[Seq[X]\]
    */
  def sequence[X](xts: Seq[Try[X]]): Try[Seq[X]] =
    xts.foldLeft(Try(Seq[X]())) { (xst, xt) => for (xs <- xst; x <- xt) yield xs :+ x }

  /**
    * Method sequence to convert a tuple of A, Try[B] to a Try[(A, B)]
    *
    * @param t the tuple.
    * @tparam A the underlying type of the _1 element of t.
    * @tparam B the underlying type of the _2 element of t.
    * @return a Try of (A, B).
    */
  def sequence[A, B](t: (A, Try[B])): Try[(A, B)] = t match
    case (a, Success(b)) => Success(a -> b)
    case (_, Failure(x)) => Failure(x)

  /**
    * Method sequenceLeftRight which, given a Map[K,Either[X,V]\], returns a tuple of sequenced maps (each with the same key type), with the X values on the left and the V values on the right.
    *
    * @param vXeKm the map
    * @tparam K the key type
    * @tparam V the value type
    * @tparam X the partition type
    * @return the separated maps as a tuple of sequenced maps
    */
  def sequenceLeftRight[K, V, X](vXeKm: Map[K, Either[X, V]]): (Seq[(K, X)], Seq[(K, V)]) =
    tupleMap[Seq[(K, Either[X, V])], Seq[(K, X)], Seq[(K, Either[X, V])], Seq[(K, V)]](sequenceLeft, sequenceRight)(partition(vXeKm))

  /**
    * Method sequenceLeft which, given a Map[K,Either[X,V]\] (in sequential form), returns a Map[K,X] (also in sequential form) for those elements of the input map which are a (left) X (as opposed to a (right) V).
    *
    * @param xVeKs a Map[K,Either[X,V]\] (in sequential form)
    * @tparam K the key type
    * @tparam X the partition type
    * @tparam V the value type
    * @return a Map[K,X] (in sequential form)
    */
  def sequenceLeft[K, X, V](xVeKs: Seq[(K, Either[X, V])]): Seq[(K, X)] =
    sequenceRight[K, V, X](for ((k, e) <- xVeKs) yield (k, e.swap))

  /**
    * Method sequenceRight which, given a Map[K,Either[X,V]\] (in sequential form), returns a Map[K,V] (also in sequential form) for those elements of the input map which are a (right) V (as opposed to a (left) X).
    *
    * @param xVeKs a Map[K,Either[X,V]\] (in sequential form)
    * @tparam K the key type
    * @tparam V the value type
    * @tparam X the partition type
    * @return a Map[K,V] (in sequential form)
    */
  def sequenceRight[K, X, V](xVeKs: Seq[(K, Either[X, V])]): Seq[(K, V)] =
    for
      (k, e) <- xVeKs
      if e.isRight
    yield k -> e.getOrElse(0.asInstanceOf[V])

  /**
    * Method toMap which takes a tuple of sequenced maps and returns a tuple of actual maps (each map has the same key type but different value types)
    *
    * @param t the input tuple
    * @tparam K the key type
    * @tparam V the value type
    * @tparam X the partition type
    * @return the output tuple
    */
  def toMap[K, V, X](t: (Seq[(K, X)], Seq[(K, V)])): (Map[K, X], Map[K, V]) =
    (t._1.toMap, t._2.toMap)

  /**
   * Transforms each element of a tuple by applying the provided functions to its components.
   *
   * @param fl a function to transform the first element of the tuple.
   * @param fr a function to transform the second element of the tuple.
   * @param t  a tuple containing two elements to be transformed.
   * @tparam L1 the type of the first element in the input tuple.
   * @tparam L2 the type of the first element in the output tuple.
   * @tparam R1 the type of the second element in the input tuple.
   * @tparam R2 the type of the second element in the output tuple.
   * @return a new tuple where the first element is transformed by `fl`, and the second by `fr`.
   */
  def tupleMap[L1, L2, R1, R2](fl: L1 => L2, fr: R1 => R2)(t: (L1, R1)): (L2, R2) =
    (fl(t._1), fr(t._2))

  /**
   * Converts a lazy `Try[X]` into a `Future[X]`.
   *
   * If the `Try` is a `Success`, the resulting `Future` will be successfully completed with the same value.
   * If the `Try` is a `Failure`, the resulting `Future` will be failed with the same exception.
   *
   * @param xy a lazy computation producing a `Try[X]` instance, which may either be `Success` containing a value or `Failure` containing an exception.
   * @tparam X the type of the value contained within the `Try`.
   * @return a `Future[X]` that is completed based on the outcome of the provided `Try`.
   */
  def asFuture[X](xy: => Try[X]): Future[X] = xy match
    case Success(s) => Future.successful(s)
    case Failure(e) => Future.failed(e)

  /**
    * Method to invoke a function (T1,T2)=>R on a tuple (T1, T2).
    *
    * @param t the tuple.
    * @param f the function.
    * @tparam T1 the type of the first attribute of t.
    * @tparam T2 the type of the second attribute of t.
    * @tparam R  the type of the result.
    * @return the result of invoking f on t.
    */
  def invokeTupled[T1, T2, R](t: (T1, T2))(f: (T1, T2) => R): R = f.tupled(t)

  /**
    * Method to make a compatibility check on two vectors (not currently used).
    * The result is successful if the vectors are of the same (non-zero) size.
    *
    * @param as a vector of As.
    * @param bs a vector of Bs.
    * @tparam A the underlying type of as.
    * @tparam B the underlying type of bs.
    * @return a tuple of the two vectors, all wrapped in Try.
    */
  def checkCompatible[A, B](as: Seq[A], bs: Seq[B]): Try[(Seq[A], Seq[B])] =
    if as.size == bs.size && as.nonEmpty
    then Success((as, bs))
    else Failure(IncompatibleLengthsException(as.size, bs.size))

  /**
    * Method to make a compatibility check on a vector and a 2-matrix (not currently used).
    * The result is successful if the vectors are of the same (non-zero) size.
    *
    * @param as  a vector of As, represented as a Seq[A].
    * @param bss a 2-matrix of Bs, represented as a Seq[Seq[B]\].
    * @tparam A the underlying type of as.
    * @tparam B the underlying type of bss.
    * @return a tuple of the vector and the transpose of the 2-matrix, all wrapped in Try.
    */
  def checkCompatibleX[A, B](as: Seq[A], bss: Seq[Seq[B]]): Try[(Seq[A], Seq[Seq[B]])] =
    checkCompatible(as, bss.transpose)
