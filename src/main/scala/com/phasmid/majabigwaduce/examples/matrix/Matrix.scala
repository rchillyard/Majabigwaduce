package com.phasmid.majabigwaduce.examples.matrix

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.util.Timeout
import com.phasmid.laScala.fp.Spy
import com.phasmid.majabigwaduce._
import com.phasmid.majabigwaduce.examples.CountWords.getTimeout
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.Random

case class MatrixOperation[X: Numeric](keyFunc: Int => Int)(implicit system: ActorSystem, logger: LoggingAdapter, config: Config, timeout: Timeout, ec: ExecutionContext) extends ((Seq[Seq[X]], Seq[X]) => Future[Seq[X]]) {

  self =>

  type XS = Seq[X]

  override def apply(xss: Seq[XS], ys: XS): Future[XS] = {
    implicit object zeroSeqX$$ extends Zero[XS] {
      def zero: XS = Seq[X]()
    }
    val s1: MapReduce[(Int, XS), Int, X] = MapReducePipe[Int, XS, Int, X, X](
      (i, xs) => (keyFunc(i), Matrix.dot(xs, ys)),
      (a, x) => implicitly[Numeric[X]].plus(a, x),
      1
    )
    val r = Reduce[Int, X, XS]((x, y) => y +: x)
    val mr = s1 | r
    mr((xss zipWithIndex) map { t: (XS, Int) => t swap })
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
      (i, xs) => (keyFunc(i), Matrix.product(xs, yss)),
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

object Matrix extends App {

  // TODO This is redundant
  trait DoubleZero$ extends Zero[Double] {
    def zero: Double = 0
  }

  implicit object DoubleZero$ extends DoubleZero$

  implicit val spyLogger: org.slf4j.Logger = Spy.getLogger(getClass)

  def dot[X: Numeric](as: Seq[X], bs: Seq[X]): X = {
    def product(ab: (X, X)): X = implicitly[Numeric[X]].times(ab._1, ab._2)

    Spy.spy(s"dot($as,$bs) = ", ((as zip bs) map product).sum)
  }

  def product[X: Numeric](as: Seq[X], bss: Seq[Seq[X]]): Seq[X] = for (bs <- bss) yield Spy.spy(s"product $as x $bs", dot(as, bs))

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
