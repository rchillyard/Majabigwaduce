/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.util.Timeout
import com.phasmid.majabigwaduce.examples.matrix.MatrixOperation
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest._
import org.scalatest.concurrent._
import org.scalatest.matchers.should
import org.scalatest.time.{Seconds, Span}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.postfixOps

class MatrixOperationFuncSpec extends flatspec.AnyFlatSpec with should.Matchers with Futures with ScalaFutures with Inside {
  "MatrixOperation" should "apply vector" in {
    implicit val config: Config = ConfigFactory.load.getConfig("Matrix")
    implicit val system: ActorSystem = ActorSystem(config.getString("name"))
    implicit val to: Timeout = getTimeout(config.getString("timeout"))
    implicit val logger: LoggingAdapter = system.log
    import ExecutionContext.Implicits.global
    val op: MatrixOperation[Int] = MatrixOperation(x => x % 10)
    val matrix = Seq(Seq(1, 1), Seq(2, 1))
    val vector = Seq(3, 5)
    val isf: Future[Seq[Int]] = op(matrix, vector)

    whenReady(isf, timeout(Span(300, Seconds))) {
      is: Seq[Int] =>
        val ok = for (i1 <- is.headOption; i2 <- is.tail.headOption) yield i1 == 8 && i2 == 11
        ok should matchPattern { case Some(true) => }
    }

    Await.ready(system.terminate(), 5 seconds)
  }

  it should "create product of matrices" in {
    implicit val config: Config = ConfigFactory.load.getConfig("Matrix")
    implicit val system: ActorSystem = ActorSystem(config.getString("name"))
    implicit val to: Timeout = getTimeout(config.getString("timeout"))
    implicit val logger: LoggingAdapter = system.log
    import ExecutionContext.Implicits.global
    val op: MatrixOperation[Int] = MatrixOperation(x => x % 10)
    val matrix1 = Seq(Seq(1, 2, 3), Seq(4, 5, 6))
    val matrix2 = Seq(Seq(7, 8), Seq(9, 10), Seq(11, 12))
    val isf: Future[Seq[Seq[Int]]] = op.product(matrix1, matrix2)

    whenReady(isf, timeout(Span(300, Seconds))) {
      is: Seq[Seq[Int]] => assert(is.head == Seq(58, 64) && is.tail.head == Seq(139, 154))
    }

    Await.ready(system.terminate(), 5 seconds)
  }

  def getTimeout(t: String): Timeout = {
    val durationR = """(\d+)\s*(\w+)""".r
    t match {
      case durationR(n, s) => new Timeout(FiniteDuration(n.toLong, s))
      case _ => Timeout(10 seconds)
    }
  }

}


