package com.phasmid.majabigwaduce.examples.matrix

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.util.Timeout
import com.phasmid.majabigwaduce._
import com.phasmid.majabigwaduce.examples.CountWords.getTimeout

import scala.concurrent.duration._
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.immutable
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.Random

case class MatrixOperation[X: Numeric](keyFunc: Int=>Int)(implicit system: ActorSystem, logger: LoggingAdapter, config: Config, timeout: Timeout, ec: ExecutionContext) extends ((Seq[Seq[X]],Seq[X]) => Future[Seq[X]]) {

  override def apply(xss: Seq[Seq[X]], vector: Seq[X]): Future[Seq[X]] = {
    val xn = implicitly[Numeric[X]]

    def f(i: Int, xs: Seq[X]): (Int, X) = (keyFunc(i), Matrix.dot(xs, vector))
    def g(a: X, x: X) = xn.plus(a,x)
        val s1: MapReduce[(Int, Seq[X]), Int,X] = MapReducePipe[Int,Seq[X],Int,X,X](
          f,
          g,
          1
        )
    val r: Reduce[Int, X, Seq[X]] = Reduce[Int, X, Seq[X]](init _)(addElement)
    val mr: ASync[Seq[(Int, Seq[X])], Seq[X]] = s1 | r

    mr((xss zipWithIndex) map { t: (Seq[X], Int) => t swap })
  }

  private def init = Seq[X]()

  private def addElement(x: Seq[X], y: X) = y +: x
}

object Matrix extends App {

  def dot[X: Numeric](as: Seq[X], bs: Seq[X]): X = {
    val xn = implicitly[Numeric[X]]
    def product(ab: (X,X)): X = xn.times(ab._1,ab._2)
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
  def row(i: Int): Seq[Double] = {val r = new Random(i); (Stream.from(0) take cols) map (x => r.nextDouble())}
  val matrix: Seq[Seq[Double]] = Stream.tabulate(rows)(row)
  val vector: Seq[Double] = row(-1)
  val isf: Future[Seq[Double]] = op(matrix,vector)
  Await.result(isf, 10.minutes)
  isf foreach println
  system.terminate()
}
