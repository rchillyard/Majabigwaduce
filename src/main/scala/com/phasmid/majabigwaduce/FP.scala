/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Either, Failure, Left, Right, Success, Try}

/**
  * This module contains functional programming methods which can also be found in the FP module of LaScala
  */
object FP {

  /**
    * Method flatten which, applied to a Future[Try[X]\], returns a Future[X]
    *
    * @param xyf      the input
    * @param executor the (implicit) execution context
    * @tparam X the underlying type
    * @return a future X
    */
  def flatten[X](xyf: Future[Try[X]])(implicit executor: ExecutionContext): Future[X] = for (xy <- xyf; x <- asFuture(xy)) yield x

  /**
    * Method to take a Map[K,Either[X,V]\] and generated a tuple of two sequenced-maps, each of the same form as the input but containing only the left-values or right-values as appropriate.
    *
    * @param vXeKm the input map
    * @tparam K the key type
    * @tparam V the value type
    * @tparam X the partition type
    * @return a tuple of Map[K,Either[X,V]\] maps in sequenced form.
    **/
  def partition[K, V, X](vXeKm: Map[K, Either[X, V]]): (Seq[(K, Either[X, V])], Seq[(K, Either[X, V])]) = vXeKm.toSeq.partition({ case (_, v) => v.isLeft })

  /**
    * Method sequence which applied to a Try[X] returns an Either[Throwable,X].
    *
    * @param xt : Try[X]
    * @tparam X the underlying type
    * @return : Either[Throwable,X]
    */
  def sequence[X](xt: Try[X]): Either[Throwable, X] = xt match {
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
    // TODO fix deprecation
  def sequence[X](xts: Seq[Try[X]]): Try[Seq[X]] = (Try(Seq[X]()) /: xts) { (xst, xt) => for (xs <- xst; x <- xt) yield xs :+ x }

  /**
    * Method sequence to separate out the left and right parts of a map of Either's.
    *
    * @param vXeKm a Map[K,Either[X,V]\]
    * @tparam K the key type
    * @tparam V the value type
    * @tparam X the partition type
    * @return a tuple of two maps, a Map[K,X] and a Map[K,V]
    */
  def sequence[K, V, X](vXeKm: Map[K, Either[X, V]]): (Map[K, X], Map[K, V]) = toMap(sequenceLeftRight(vXeKm))

  /**
    * Method sequence to convert a tuple of A, Try[B] to a Try[(A, B)]
    *
    * @param t the tuple.
    * @tparam A the underlying type of the _1 element of t.
    * @tparam B the underlying type of the _2 element of t.
    * @return a Try of (A, B).
    */
  def sequence[A, B](t: (A, Try[B])): Try[(A, B)] = t match {
    case (a, Success(b)) => Success(a -> b)
    case (_, Failure(x)) => Failure(x)
  }

  /**
    * Method sequenceInverted to convert a tuple of Try[A], B to a Try[(A, B)]
    *
    * @param t the tuple.
    * @tparam A the underlying type of the _1 element of t.
    * @tparam B the underlying type of the _2 element of t.
    * @return a Try of (A, B).
    */
  def sequenceInverted[A, B](t: (Try[A], B)): Try[(A, B)] = sequence(t.swap).map(_.swap)

  /**
    * Method sequenceLeftRight which, given a Map[K,Either[X,V]\], returns a tuple of sequenced maps (each with the same key type), with the X values on the left and the V values on the right.
    *
    * @param vXeKm the map
    * @tparam K the key type
    * @tparam V the value type
    * @tparam X the partition type
    * @return the separated maps as a tuple of sequenced maps
    */
  def sequenceLeftRight[K, V, X](vXeKm: Map[K, Either[X, V]]): (Seq[(K, X)], Seq[(K, V)]) = tupleMap[Seq[(K, Either[X, V])], Seq[(K, X)], Seq[(K, Either[X, V])], Seq[(K, V)]](sequenceLeft, sequenceRight)(partition(vXeKm))

  /**
    * Method sequenceLeft which, given a Map[K,Either[X,V]\] (in sequential form), returns a Map[K,X] (also in sequential form) for those elements of the input map which are a (left) X (as opposed to a (right) V).
    *
    * @param vXeKs a Map[K,Either[X,V]\] (in sequential form)
    * @tparam K the key type
    * @tparam V the value type
    * @tparam X the partition type
    * @return a Map[K,X] (in sequential form)
    */
  // TODO fix deprecation
  def sequenceLeft[K, V, X](vXeKs: Seq[(K, Either[X, V])]): Seq[(K, X)] = for ((k, e) <- vXeKs) yield (k, e.left.get)

  /**
    * Method sequenceRight which, given a Map[K,Either[X,V]\] (in sequential form), returns a Map[K,V] (also in sequential form) for those elements of the input map which are a (right) V (as opposed to a (left) X).
    *
    * @param vXeKs a Map[K,Either[X,V]\] (in sequential form)
    * @tparam K the key type
    * @tparam V the value type
    * @tparam X the partition type
    * @return a Map[K,V] (in sequential form)
    */
  // TODO fix deprecations
  def sequenceRight[K, V, X](vXeKs: Seq[(K, Either[X, V])]): Seq[(K, V)] = for ((k, e) <- vXeKs) yield (k, e.right.get)

  /**
    * Method toMap which takes a tuple of sequenced maps and returns a tuple of actual maps (each map has the same key type but different value types)
    *
    * @param t the input tuple
    * @tparam K the key type
    * @tparam V the value type
    * @tparam X the partition type
    * @return the output tuple
    */
  def toMap[K, V, X](t: (Seq[(K, X)], Seq[(K, V)])): (Map[K, X], Map[K, V]) = (t._1.toMap, t._2.toMap)

  /**
    * Method tupleMap which, given a left-function and a right-function, operates on a tuple, returning a new tuple with each component transformed by the appropriate function.
    *
    * @param fl the left-function
    * @param fr the right-function
    * @param t  a tuple
    * @tparam L1 the left function parameter type
    * @tparam L2 the left function result type
    * @tparam R1 the right function parameter type
    * @tparam R2 the right function result type
    * @return the tuple transformed by the appropriate functions
    */
  def tupleMap[L1, L2, R1, R2](fl: L1 => L2, fr: R1 => R2)(t: (L1, R1)): (L2, R2) = (fl(t._1), fr(t._2))

  private def asFuture[X](xy: => Try[X]): Future[X] = xy match {
    case Success(s) => Future.successful(s)
    case Failure(e) => Future.failed(e)
  }

  /**
    * Lift function to transform a function f of type T=>R into a function of type Seq[T]=>Seq[R]
    *
    * @param f the function we start with, of type T=>R
    * @tparam T the type of the parameter to f
    * @tparam R the type of the result of f
    * @return a function of type Seq[T]=>Seq[R]
    */
  def lift[T, R](f: T => R): Seq[T] => Seq[R] = _ map f

  /**
    * Lift function to transform a function f of type (T1,T2)=>R into a function of type (Seq[T1],Seq[T2])=>Seq[R]
    *
    * @param f the function we start with, of type (T1,T2)=>R
    * @tparam T1 the type of the first parameter to f
    * @tparam T2 the type of the second parameter to f
    * @tparam R  the type of the result of f
    * @return a function of type (Seq[T1],Seq[T2])=>Seq[R]
    */
  def lift2[T1, T2, R](f: (T1, T2) => R): (Seq[T1], Seq[T2]) => Seq[R] = map2(_, _)(f)

  /**
    * The map2 function for Seq
    *
    * @param t1y parameter 1 wrapped in Seq
    * @param t2y parameter 2 wrapped in Seq
    * @param f   function that takes two parameters of types T1 and T2 and returns a value of R
    * @tparam T1 the type of parameter 1
    * @tparam T2 the type of parameter 2
    * @tparam R  the type of the result of function f
    * @return a value of R, wrapped in Seq
    */
  def map2[T1, T2, R](t1y: Seq[T1], t2y: Seq[T2])(f: (T1, T2) => R): Seq[R] =
    for {
      t1 <- t1y
      t2 <- t2y
    } yield f(t1, t2)

  /**
    *
    * @param t1y parameter 1 wrapped in Try
    * @param t2y parameter 2 wrapped in Try
    * @param f   function that takes two parameters of types T1 and T2 and returns a value of R
    * @tparam T1 the type of parameter 1
    * @tparam T2 the type of parameter 2
    * @tparam R  the type of the result of function f
    * @return a value of R, wrapped in Try
    */
  def map2[T1, T2, R](t1y: Try[T1], t2y: Try[T2])(f: (T1, T2) => R): Try[R] = for {
    t1 <- t1y
    t2 <- t2y
  } yield f(t1, t2)


}
