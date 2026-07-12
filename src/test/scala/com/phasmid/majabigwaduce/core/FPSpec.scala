package com.phasmid.majabigwaduce.core

import com.phasmid.majabigwaduce.core.FP.*
import org.scalatest.*
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

  behavior of "whenTry"
  it should "succeed when the condition is true" in {
    whenTry(true)(42) shouldBe Success(42)
  }
  it should "fail when the condition is false" in {
    whenTry(false)(42) should matchPattern { case Failure(_) => }
  }

  behavior of "wheneverTry"
  it should "flatten a Success when the condition is true" in {
    wheneverTry(true)(Success(42)) shouldBe Success(42)
  }
  it should "flatten a Failure when the condition is true" in {
    val x = MapReduceException("boom")
    wheneverTry(true)(Failure(x)) shouldBe Failure(x)
  }
  it should "fail when the condition is false" in {
    wheneverTry(false)(Success(42)) should matchPattern { case Failure(_) => }
  }

  behavior of "whenever"
  it should "flatten a Some when the condition is true" in {
    whenever(true)(Some(42)) shouldBe Some(42)
  }
  it should "flatten a None when the condition is true" in {
    whenever(true)(None) shouldBe None
  }
  it should "yield None when the condition is false" in {
    whenever(false)(Some(42)) shouldBe None
  }

  behavior of "recover"
  it should "return the value when Some" in {
    recover(Some(42))(new Exception("boom")) shouldBe 42
  }
  it should "throw the given exception when None" in {
    val x = new Exception("boom")
    a[Exception] should be thrownBy recover(None)(x)
  }

  behavior of "recoverAsTry"
  it should "return a Success when Some" in {
    recoverAsTry(Some(42))(new Exception("boom")) shouldBe Success(42)
  }
  it should "return a Failure when None" in {
    val x = new Exception("boom")
    recoverAsTry(None)(x) shouldBe Failure(x)
  }

  behavior of "recoverWithTry"
  it should "return a Success when Some" in {
    recoverWithTry(Some(42))(Failure(new Exception("boom"))) shouldBe Success(42)
  }
  it should "fall back to the given Try when None" in {
    val x = new Exception("boom")
    recoverWithTry(None)(Failure(x)) shouldBe Failure(x)
    recoverWithTry(None: Option[Int])(Success(99)) shouldBe Success(99)
  }

  behavior of "toTry"
  it should "return a Success when Some" in {
    toTry(Some(42))(Failure(new Exception("boom"))) shouldBe Success(42)
  }
  it should "fall back to the default when None" in {
    val x = new Exception("boom")
    toTry(None)(Failure(x)) shouldBe Failure(x)
  }

  behavior of "toOption"
  it should "return Some for a Success" in {
    toOption(Success(42)) shouldBe Some(42)
  }
  it should "return None for a Failure" in {
    toOption(Failure(new Exception("boom"))) shouldBe None
  }

  behavior of "toOptionWithLog"
  it should "return Some for a Success and never invoke the log" in {
    var logged = false
    toOptionWithLog[Int](_ => logged = true)(Success(42)) shouldBe Some(42)
    logged shouldBe false
  }
  it should "return None for a Failure and invoke the log with the exception" in {
    val x = new Exception("boom")
    var logged: Option[Throwable] = None
    toOptionWithLog[Int](t => logged = Some(t))(Failure(x)) shouldBe None
    logged shouldBe Some(x)
  }

  behavior of "identityTry"
  it should "wrap a value in a Success" in {
    identityTry(42) shouldBe Success(42)
  }

  behavior of "optional"
  it should "return Some when the predicate holds" in {
    optional[Int](_ > 0)(42) shouldBe Some(42)
  }
  it should "return None when the predicate fails" in {
    optional[Int](_ > 0)(-1) shouldBe None
  }

  behavior of "sequence(Option[Try[X]])"
  it should "return Success(None) for None" in {
    sequence(None: Option[Try[Int]]) shouldBe Success(None)
  }
  it should "return Success(Some(x)) for Some(Success(x))" in {
    sequence(Some(Success(42))) shouldBe Success(Some(42))
  }
  it should "return Failure(x) for Some(Failure(x))" in {
    val x = MapReduceException("boom")
    sequence(Some(Failure(x)): Option[Try[Int]]) shouldBe Failure(x)
  }

  behavior of "sequence(Iterable[Option[X]])"
  it should "return Some(Seq(...)) when all elements are Some" in {
    sequence(Seq(Some(1), Some(2), Some(3))) shouldBe Some(Seq(1, 2, 3))
  }
  it should "return None when any element is None" in {
    sequence(Seq(Some(1), None, Some(3))) shouldBe None
  }
  it should "return Some(Nil) for an empty input" in {
    sequence(Seq.empty[Option[Int]]) shouldBe Some(Nil)
  }

  behavior of "sequence(Iterator[Option[X]])"
  it should "return Some(Iterator(...)) when all elements are Some" in {
    sequence(Iterator(Some(1), Some(2), Some(3))).map(_.toSeq) shouldBe Some(Seq(1, 2, 3))
  }
  it should "return None when any element is None" in {
    sequence(Iterator(Some(1), None, Some(3))) shouldBe None
  }
}

class TryUsingSpec extends flatspec.AnyFlatSpec with matchers.should.Matchers {

  behavior of "TryUsing"

  class Resource(value: Int) extends AutoCloseable {
    var closed: Boolean = false

    def get: Int = value

    def close(): Unit = closed = true
  }

  it should "flatten a Success(Success(...)) into a Success and close the resource" in {
    val resource = new Resource(42)
    val result = TryUsing(resource)(r => Success(r.get * 2))
    result shouldBe Success(84)
    resource.closed shouldBe true
  }

  it should "flatten a Success(Failure(...)) into a Failure and still close the resource" in {
    val resource = new Resource(1)
    val x = MapReduceException("boom")
    val result = TryUsing(resource)(_ => Failure(x))
    result shouldBe Failure(x)
    resource.closed shouldBe true
  }

  it should "fail (and never invoke f) if constructing the resource throws" in {
    def resource: Resource = throw new RuntimeException("cannot construct")

    var invoked = false
    val result = TryUsing(resource) { r => invoked = true; Success(r.get) }
    result should matchPattern { case Failure(_) => }
    invoked shouldBe false
  }

  it should "short-circuit (and never construct the resource) when given an already-failed Try[R]" in {
    val x = MapReduceException("upstream failure")
    var constructed = false

    def resource: Resource = { constructed = true; new Resource(1) }

    val result = TryUsing(Failure(x): Try[Resource])(r => Success(r.get))
    result shouldBe Failure(x)
    constructed shouldBe false
  }

  it should "delegate to the resource-based apply when given a successful Try[R]" in {
    val resource = new Resource(7)
    val result = TryUsing(Success(resource): Try[Resource])(r => Success(r.get + 1))
    result shouldBe Success(8)
    resource.closed shouldBe true
  }
}
