/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce.examples.matrix

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.util.Timeout
import com.phasmid.majabigwaduce._
import com.phasmid.majabigwaduce.examples.CountWords.getTimeout
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.{Failure, Random, Try}

case class MatrixOperation[X: Numeric](keyFunc: Int => Int)(implicit system: ActorSystem, logger: LoggingAdapter, config: Config, timeout: Timeout, ec: ExecutionContext) extends ((Seq[Seq[X]], Seq[X]) => Future[Seq[X]]) {

  self =>

  type XS = Seq[X]
  type Row = (Int, XS)
  type Element = (Int, X)
  type Vector = Map[Int, X]

  override def apply(xss: Seq[XS], ys: XS): Future[XS] = {
    implicit object zeroVector$$ extends Zero.VectorZero[X]
    val s1 = MapReduceFirstFold[Row, Int, Element, Vector](
      { case (i, xs) => keyFunc(i) -> (i -> MatrixOperation.dot(xs, ys)) },
      { case (v, (i, x)) => v + (i -> x) }
    )(config, system, timeout)
    val r = Reduce[Int, Vector, Vector] { case (s, t) => s ++ t }
    val mr = s1 | r
    val z = (xss zipWithIndex) map (_ swap)
    // CONSIDER doing this as another stage of map-reduce (or part of r stage).
    FP.flatten(for (q <- mr(z)) yield mapVectorToXS(q, xss.length))
  }

  private def mapVectorToXS(q: Vector, n: Int): Try[XS] = {
    val keys = q.keySet
    if (keys.size == n) {
      val z = for (i <- 0 until n) yield q(i)
      Try(z.foldLeft(Seq[X]())((b, x) => b :+ x))
    }
    else Failure(MapReduceException(s"mapVectorToXS: incorrect count: ${keys.size}, $n"))
  }

  //  def product(xss: Seq[XS], yss: Seq[XS]): Future[Seq[XS]] = Future.sequence(for (ys <- yss) yield self(xss, ys))
  def product(xss: Seq[XS], yss: Seq[XS]): Future[Seq[XS]] = {
    implicit object zeroSeqX$$ extends Zero[XS] {
      def zero: XS = Seq[X]()
    }
    implicit object zeroSeqSeqX$$ extends Zero[Seq[XS]] {
      def zero: Seq[XS] = Seq[Seq[X]]()
    }
    val s1: MapReduce[(Int, XS), Int, XS] = MapReducePipe[Int, XS, Int, XS, XS](
      (i, xs) => (keyFunc(i), MatrixOperation.product(xs, yss)),
      (zs: XS, xs: XS) => for ((x, y) <- zs zip xs) yield implicitly[Numeric[X]].plus(x, y),
      1
    )
    val r = Reduce[Int, XS, Seq[XS]]((x, y) => y +: x)
    val mr = s1 | r
    val zs: Seq[(XS, Int)] = xss zipWithIndex

    val tuples: Seq[(Int, XS)] = zs map { t: (XS, Int) => t swap }
    mr(tuples)

    //    def add(t: (X, X)): X = implicitly[Numeric[X]].plus(t._1,t._2)
  }

}

object MatrixOperation extends App {

  // TODO This is redundant
  trait DoubleZero$ extends Zero[Double] {
    def zero: Double = 0
  }

  implicit object DoubleZero$ extends DoubleZero$

  def dot[X: Numeric](as: Seq[X], bs: Seq[X]): X = {
    def product(ab: (X, X)): X = implicitly[Numeric[X]].times(ab._1, ab._2)

    ((as zip bs) map product).sum
  }

  def product[X: Numeric](as: Seq[X], bss: Seq[Seq[X]]): Seq[X] = for (bs <- bss.transpose) yield dot(as, bs)

  implicit val config: Config = ConfigFactory.load.getConfig("Matrix")
  implicit val system: ActorSystem = ActorSystem(config.getString("name"))
  implicit val timeout: Timeout = getTimeout(config.getString("timeout"))
  val rows = config.getInt("rows")
  val cols = config.getInt("columns")
  val modulus = config.getInt("modulus")
  implicit val logger: LoggingAdapter = system.log

  import ExecutionContext.Implicits.global

  val op: MatrixOperation[Double] = MatrixOperation(x => x % modulus)

  def row(i: Int): Seq[Double] = {
    val r = new Random(i)
    (LazyList.from(0) take cols) map (_ => r.nextDouble())
  }

  val matrix: Seq[Seq[Double]] = LazyList.tabulate(rows)(row)
  val vector: Seq[Double] = row(-1)
  val isf: Future[Seq[Double]] = op(matrix, vector)
  Await.result(isf, 10.minutes)
  isf foreach println
  system.terminate()
}
