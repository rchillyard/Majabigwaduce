package com.phasmid.majabigwaduce.core

import com.phasmid.majabigwaduce.core.FP._
import org.scalatest._
import org.scalatest.concurrent.{Futures, ScalaFutures}

import java.net.URL
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class FPSpec extends flatspec.AnyFlatSpec with matchers.should.Matchers with Futures with ScalaFutures {
  behavior of "FP"

  it should "sequence" in {
    val try1 = Success(1)
    val try2 = Success(2)
    val try3 = Failure(MapReduceException(""))
    sequence(Seq(try1, try2)) shouldBe Success(Seq(1, 2))
    sequence(Seq(try1, try3)) should matchPattern { case Failure(_) => }
  }

  it should "work for sequenceRight" in {
    val map: Map[String, Either[Int, Double]] = Map("x" -> Left(1), "y" -> Right(1.0))
    sequenceRight(map.toSeq) shouldBe Seq("y" -> 1.0)
  }

  it should "work for sequenceLeft" in {
    val map: Map[String, Either[Int, Double]] = Map("x" -> Left(1), "y" -> Right(1.0))
    sequenceLeft(map.toSeq) shouldBe Seq("x" -> 1)
  }

  behavior of "checkCompatible"
  it should "succeed for a sequence and itself" in {
    val xs = Seq(1, 2, 3)
    checkCompatible(xs, xs) should matchPattern { case Success((`xs`, `xs`)) => }
  }
  it should "fail when any parameter is Nil" in {
    val xs = Seq(1, 2, 3)
    checkCompatible(xs, Nil) should matchPattern { case Failure(_) => }
    checkCompatible(Nil, xs) should matchPattern { case Failure(_) => }
    checkCompatible(Nil, Nil) should matchPattern { case Failure(_) => }
  }
  it should "fail for a sequence and something of different length" in {
    val xs = Seq(1, 2, 3)
    checkCompatible(xs, xs take 2) should matchPattern { case Failure(_) => }
    checkCompatible(xs take 2, xs) should matchPattern { case Failure(_) => }
  }

  behavior of "checkCompatibleX"
  it should "succeed for a sequence and itself" in {
    val xs = Seq(1, 2, 3)
    val yss = Seq(Seq(1, 2, 3), Seq(1, 2, 3), Seq(1, 2, 3))
    checkCompatibleX(xs, yss) should matchPattern { case Success((`xs`, _)) => }
  }
  it should "fail when any parameter is Nil" in {
    val xs = Seq(1, 2, 3)
    val yss = Seq(Seq(1, 2, 3), Seq(1, 2, 3), Seq(1, 2, 3))
    checkCompatibleX(xs, Nil) should matchPattern { case Failure(_) => }
    checkCompatibleX(Nil, yss) should matchPattern { case Failure(_) => }
    checkCompatibleX(Nil, Nil) should matchPattern { case Failure(_) => }
  }
  it should "fail for a sequence and something of different length" in {
    val xs = Seq(1, 2, 3)
    val yss = Seq(Seq(1, 2), Seq(1, 2), Seq(1, 2))
    checkCompatibleX(xs, yss) should matchPattern { case Failure(_) => }
  }

  "lift(Future[Try[T]])" should "succeed for https://www.google.com" in {
    val uyf = Future(Try(new URL("https://www.google.com")))
    val uf = flatten(uyf)
    whenReady(uf) { u => u should matchPattern { case _: URL => } }
  }

  "lift(Try[Future[T]])" should "succeed for https://www.google.com" in {
    val ufy: Try[Future[URL]] = Try(Future(new URL("https://www.google.com")))
    val uf: Future[URL] = flatten(ufy)
    whenReady(uf) { u => u should matchPattern { case _: URL => } }
  }

  "sequence(Seq[Future[T]])" should "succeed for https://www.google.com, etc." in {
    val ws = List("https://www.google.com", "https://www.microsoft.com")
    val ufs: Seq[Future[URL]] = for {w <- ws; uf = Future(new URL(w))} yield uf
    val usf: Future[Seq[URL]] = Future.sequence(ufs)
    whenReady(usf) { us => Assertions.assert(us.length == 2) }
  }

  behavior of "sequence(Seq[Try[T]])"
  it should "succeed for https://www.google.com, etc." in {
    val ws = List("https://www.google.com", "https://www.microsoft.com")
    val uys = for {w <- ws; url = Try(new URL(w))} yield url
    sequence(uys) match {
      case Success(us) => Assertions.assert(us.length == 2)
      case _ => Failed
    }
  }
  it should "fail for www.google.com, etc." in {
    val ws = List("www.google.com", "https://www.microsoft.com")
    val uys = for {w <- ws; uy = Try(new URL(w))} yield uy
    sequence(uys) match {
      case Failure(_) => Succeeded
      case _ => Failed
    }
  }
  it should "succeed for empty list" in {
    val uys = for {w <- List[String](); uy = Try(new URL(w))} yield uy
    sequence(uys) match {
      case Success(us) => Assertions.assert(us.isEmpty)
      case _ => Failed
    }
  }

  "lift" should "succeed" in {
    def double(x: Int) = 2 * x

    Success(1) map double should matchPattern { case Success(2) => }
    Failure(new Exception("bad")) map double should matchPattern { case Failure(_) => }
  }

  "asFuture" should "succeed" in {

    val eventualInt: Future[Int] = asFuture(Success(1))
    whenReady(eventualInt) { x => x should matchPattern { case 1 => } }
    //    whenReady(toFuture(Failure[Int](new Exception("bad")))) { x => p shouldBe new Exception("bad")}
  }
}
