/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.util.Timeout
import org.slf4j.{Logger, LoggerFactory}
import com.phasmid.majabigwaduce.examples.matrix.{MatrixOperation, matrixOperationApp}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.*
import org.scalatest.concurrent.*
import org.scalatest.matchers.should
import org.scalatest.time.{Seconds, Span}

import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future}

class MatrixOperationFuncSpec extends flatspec.AnyFlatSpec with should.Matchers with Futures with ScalaFutures with Inside {
  "MatrixOperation" should "apply vector" in {
    given config: Config = ConfigFactory.load.getConfig("majabigwaduce.Matrix")

    given system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, config.getString("name"))

    given to: Timeout = getTimeout(config.getString("timeout"))

    given logger: Logger = LoggerFactory.getLogger(classOf[MatrixOperation[?]])
    import ExecutionContext.Implicits.global
    val op: MatrixOperation[Int] = MatrixOperation(x => x % 10)
    val matrix = Seq(Seq(1, 1), Seq(2, 1))
    val vector = Seq(3, 5)
    val isf: Future[Seq[Int]] = op(matrix, vector)

    whenReady(isf, timeout(Span(300, Seconds))) {
      (is: Seq[Int]) =>
        val ok = for (i1 <- is.headOption; i2 <- is.tail.headOption) yield i1 == 8 && i2 == 11
        ok should matchPattern { case Some(true) => }
    }

    system.terminate()
    Await.ready(system.whenTerminated, 5.seconds)
  }

  it should "create product of matrices" in {
    given config: Config = ConfigFactory.load.getConfig("majabigwaduce.Matrix")

    given system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, config.getString("name"))

    given to: Timeout = getTimeout(config.getString("timeout"))

    given logger: Logger = LoggerFactory.getLogger(classOf[MatrixOperation[?]])
    import ExecutionContext.Implicits.global
    val op: MatrixOperation[Int] = MatrixOperation(x => x % 10)
    val matrix1 = Seq(Seq(1, 2, 3), Seq(4, 5, 6))
    val matrix2 = Seq(Seq(7, 8), Seq(9, 10), Seq(11, 12))
    val isf: Future[Seq[Seq[Int]]] = op.product(matrix1, matrix2)

    whenReady(isf, timeout(Span(300, Seconds))) {
      (is: Seq[Seq[Int]]) => assert(is.head == Seq(58, 64) && is.tail.head == Seq(139, 154))
    }

    system.terminate()
    Await.ready(system.whenTerminated, 5.seconds)
  }

  "main program" should "work" in {
    matrixOperationApp()
  }

  def getTimeout(t: String): Timeout = {
    val durationR = """(\d+)\s*(\w+)""".r
    t match {
      case durationR(n, s) => new Timeout(FiniteDuration(n.toLong, s))
      case _ => Timeout(10.seconds)
    }
  }

}


