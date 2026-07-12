package com.phasmid.majabigwaduce.core

import com.phasmid.majabigwaduce.matrix.IncompatibleLengthsException

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Using.Releasable
import scala.util.{Failure, Success, Try, Using}

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
   * TESTME
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
   * Executes the provided computation if the given condition is true; otherwise, returns a Failure.
   *
   * @param p a Boolean condition that determines whether the computation should be executed.
   * @param x a computation represented as a call-by-name parameter, which returns an X when evaluated.
   * @tparam X the underlying type.
   * @return a Try[X] representing the result of the computation if the condition is true, or a Failure if the condition is false.
   */
  def whenTry[X](p: Boolean)(x: => X): Try[X] =
    if p then Success(x) else Failure(new Exception(s"condition $p is not satisfied"))

  /**
   * Executes the provided computation if the given condition is true; otherwise, returns a Failure.
   * Unlike `whenTry`, the computation itself yields a `Try[X]`, so the result is flattened.
   *
   * @param p a Boolean condition that determines whether the computation should be executed.
   * @param x a computation represented as a call-by-name parameter, which returns a Try[X] when evaluated.
   * @tparam X the underlying type.
   * @return a Try[X] representing the result of the computation if the condition is true, or a Failure if the condition is false.
   */
  def wheneverTry[X](p: Boolean)(x: => Try[X]): Try[X] =
    whenTry(p)(x).flatten

  /**
   * Executes the provided block of code conditionally based on the given boolean predicate.
   * If the predicate is true, the block of code is executed and its result is returned.
   * If the predicate is false, None is returned.
   *
   * @param p the boolean predicate that determines whether the block of code should be executed.
   * @param x a by-name parameter representing the block of code returning an Option of type X.
   * @tparam X the underlying type.
   * @return an Option containing the result of the block of code if `p` is true, or None if `p` is false.
   */
  def whenever[X](p: Boolean)(x: => Option[X]): Option[X] =
    Option.when(p)(x).flatten

  /**
   * Method to convert a None value to a given exception (rather than the NoSuchElement exception).
   *
   * @param to an Option[T].
   * @param x  a Throwable to be thrown if to is None.
   * @tparam T the underlying type of to.
   * @return t if to is Some(t); otherwise x will be thrown.
   */
  def recover[T](to: Option[T])(x: => Throwable): T = to.getOrElse(throw x)

  /**
   * Converts an `Option` into a `Try`, providing a failure cause if the `Option` is `None`.
   *
   * @param to the optional value to convert into a `Try`.
   * @param x  the throwable to use as the failure cause if the `Option` is `None`.
   * @tparam T the underlying type of to.
   * @return a `Success` containing the value from the `Option` if it is `Some`, or a `Failure` if it is `None`.
   */
  def recoverAsTry[T](to: Option[T])(x: => Throwable): Try[T] = to match
    case Some(t) => Success(t)
    case None => Failure(x)

  /**
   * Converts an `Option` into a `Try`, falling back to the given `Try` if the `Option` is `None`.
   *
   * @param to the optional value to convert into a `Try`.
   * @param x  the `Try` to fall back to if the `Option` is `None`.
   * @tparam T the underlying type of to.
   * @return a `Success` containing the value from the `Option` if it is `Some`, or `x` if it is `None`.
   */
  def recoverWithTry[T](to: Option[T])(x: => Try[T]): Try[T] = to match
    case Some(t) => Success(t)
    case None => x

  /**
   * Method to convert an `Option` into a `Try`, given a default `Try` to use if the `Option` is `None`.
   *
   * @param xo      an Option[X].
   * @param default a Try[X] to use if `xo` is `None`.
   * @tparam X the underlying type of both input and output.
   * @return if `xo` is `Some(x)` then `Success(x)` else `default`.
   */
  def toTry[X](xo: Option[X])(default: => Try[X]): Try[X] =
    xo map (Success(_)) getOrElse default

  /**
   * Converts a `Try` instance into an `Option`. Any exception in the input is, of course, lost.
   *
   * @param xy the `Try` instance to be converted.
   * @tparam X the underlying type.
   * @return an `Option` containing the value if the `Try` is a `Success`, or `None` if the `Try` is a `Failure`.
   */
  def toOption[X](xy: => Try[X]): Option[X] =
    toOptionWithLog(_ => ())(xy)

  /**
   * Converts a `Try` instance into an `Option`. Any exception in the input is logged according to the `log` function.
   *
   * @param log a function to log the exception, if any.
   * @param xy  the `Try` instance to be converted.
   * @tparam X the underlying type.
   * @return an `Option` containing the value if the `Try` is a `Success`, or `None` if the `Try` is a `Failure`.
   */
  def toOptionWithLog[X](log: Throwable => Unit)(xy: => Try[X]): Option[X] = xy match
    case Success(x) => Some(x)
    case Failure(x) => log(x); None

  /**
   * This method is a substitute for `Try.apply` in the case that we want it as a function
   * (otherwise, we run into a type inference problem).
   *
   * @param x an X.
   * @tparam X the type of x.
   * @return Success(x)
   */
  def identityTry[X](x: X): Try[X] = Success(x)

  /**
   * Method to yield an Option of T according to whether the predicate p yields true.
   *
   * @param p a predicate on T.
   * @param t an actual value of T.
   * @tparam T the type of t (and the underlying type of the result).
   * @return Some(t) if p(t) is true, otherwise None.
   */
  def optional[T](p: T => Boolean)(t: T): Option[T] =
    Some(t).filter(p)

  /**
   * Sequence method to invert the order of types Option/Try.
   *
   * @param xyo an Option of Try[X].
   * @tparam X the underlying type.
   * @return a Try of Option[X].
   */
  def sequence[X](xyo: Option[Try[X]]): Try[Option[X]] = xyo match
    case Some(Success(x)) => Success(Some(x))
    case Some(Failure(x)) => Failure(x)
    case None => Success(None)

  /**
   * Sequence method to combine elements of type Option[X].
   *
   * @param xos an Iterable of Option[X].
   * @tparam X the underlying type.
   * @return if `xos` contains any `None`s, the result will be `None`, otherwise `Some(...)`.
   *         NOTE: that the output collection type will be Seq, regardless of the input type.
   */
  def sequence[X](xos: Iterable[Option[X]]): Option[Seq[X]] =
    xos.foldLeft(Option(Seq[X]())) {
      (xso, xo) => for xs <- xso; x <- xo yield xs :+ x
    }

  /**
   * Sequence method to combine elements of type Option[X].
   *
   * @param xos an Iterator of Option[X].
   * @tparam X the underlying type.
   * @return an Option of Iterator[X].
   */
  def sequence[X](xos: Iterator[Option[X]]): Option[Iterator[X]] =
    sequence(xos.to(List)).map(_.iterator)

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

/**
 * The `TryUsing` object provides utility methods to manage resources safely
 * and effectively using Scala's `Using` and `Try`.
 * It encapsulates resource management in a functional way, ensuring proper release of resources.
 * The methods in this object extend the functionality of `Using.apply`
 * by offering flattening operations over nested `Try`.
 */
object TryUsing:
  /**
   * This method is to `Using.apply` as `flatMap` is to `map`.
   *
   * @param resource a resource which is used by f and will be managed via `Using.apply`
   * @param f        a function of R => Try[A].
   * @tparam R the resource type.
   * @tparam A the underlying type of the result.
   * @return a Try[A]
   */
  def apply[R: Releasable, A](resource: => R)(f: R => Try[A]): Try[A] = Using(resource)(f).flatten

  /**
   * This method is similar to `apply(r)` but it takes a `Try[R]` as its parameter.
   * The definition of `f` is the same as in the other apply, however.
   *
   * TESTME
   *
   * @param ry a Try[R] which is passed into f and will be managed via `Using.apply`
   * @param f  a function of R => Try[A].
   * @tparam R the resource type.
   * @tparam A the underlying type of the result.
   * @return a Try[A]
   */
  def apply[R: Releasable, A](ry: Try[R])(f: R => Try[A]): Try[A] = for (r <- ry; a <- apply(r)(f)) yield a
