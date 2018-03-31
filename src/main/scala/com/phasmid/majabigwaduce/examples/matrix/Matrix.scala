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
import scala.util.Random

case class MatrixOperation[X: Numeric](keyFunc: Int => Int)(implicit system: ActorSystem, logger: LoggingAdapter, config: Config, timeout: Timeout, ec: ExecutionContext) extends ((Seq[Seq[X]], Seq[X]) => Future[Seq[X]]) {

  type XS = Seq[X]

  override def apply(xss: Seq[XS], vector: XS): Future[XS] = {
    implicit object zeroSeqX$$ extends Zero[XS] {
      def zero: XS = Seq[X]()
    }
    val s1: MapReduce[(Int, XS), Int, X] = MapReducePipe(
      (i, xs) => (keyFunc(i), Matrix.dot(xs, vector)),
      (a, x) => implicitly[Numeric[X]].plus(a, x),
      1
    )
    val r = Reduce[Int, X, XS]((x, y) => y +: x)
    val mr = s1 | r
    mr((xss zipWithIndex) map { t: (XS, Int) => t swap })
  }
}

object Matrix extends App {

  trait DoubleZero$ extends Zero[Double] {
    def zero: Double = 0
  }

  implicit object DoubleZero$ extends DoubleZero$

  def dot[X: Numeric](as: Seq[X], bs: Seq[X]): X = {
    def product(ab: (X, X)): X = implicitly[Numeric[X]].times(ab._1, ab._2)

    ((as zip bs) map product).sum
  }

  val configRoot = ConfigFactory.load
  implicit val config: Config = configRoot.getConfig("Matrix")
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
    (Stream.from(0) take cols) map (_ => r.nextDouble())
  }

  val matrix: Seq[Seq[Double]] = Stream.tabulate(rows)(row)
  val vector: Seq[Double] = row(-1)
  val isf: Future[Seq[Double]] = op(matrix, vector)
  Await.result(isf, 10.minutes)
  isf foreach println
  system.terminate()
}
