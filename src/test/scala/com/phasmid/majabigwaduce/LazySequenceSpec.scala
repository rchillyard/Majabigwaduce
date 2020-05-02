package com.phasmid.majabigwaduce

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class LazySequenceSpec extends AnyFlatSpec with should.Matchers {

  behavior of "LazySequence"

  val list123 = List(1, 2, 3)
  val list246 = List(2, 4, 6)

  it should "invoke LazyMonad(ts,identity,true) correctly" in {
    val l: LazyMonad[Int, Int] = LazyMonad(list123, identity, _ => true)
    l.iterator.toSeq shouldBe list123
  }
  it should "invoke LazyMonad(ts,identity) correctly" in {
    val l = LazyMonad[Int, Int](list123, identity)
    l.iterator.toSeq shouldBe list123
  }
  it should "invoke LazyMonad(ts,_*2) correctly" in {
    val l = LazyMonad[Int, Int](list123, _ * 2)
    l.iterator.toSeq shouldBe list246
  }
  it should "invoke LazyMonad(ts) correctly" in {
    val l = LazyMonad(list123)
    l.iterator.toSeq shouldBe list123
  }
  it should "invoke LazyMonad.withFilter(ts, _%2==0) correctly" in {
    val l = LazyMonad.withFilter[Int](list123, _ % 2 == 0)
    l.iterator.toSeq shouldBe List(2)
  }
  it should "invoke map(_*2) correctly" in {
    val l = LazyMonad(list123)
    l.map(_ * 2).iterator.toSeq shouldBe list246
  }
  it should "invoke preFilter(_%2==0) correctly" in {
    val l = LazyMonad(list123)
    l.preFilter(_ % 2 == 0).iterator.toSeq shouldBe List(2)
  }
  it should "invoke filter(_%2==0) correctly" in {
    val l = LazyMonad(list123)
    l.filter(_ % 2 == 0).iterator.toSeq shouldBe List(2)
  }
  it should "invoke filter(_%2!=0) on LazyMonad with function _*2 correctly" in {
    val l = LazyMonad[Int, Int](list123, _ * 2)
    l.filter(_ % 2 != 0).iterator.toSeq shouldBe Nil
  }
  it should "invoke map _*2 on LazyMonad with function _%2!=0 correctly" in {
    val l = LazyMonad.withFilter[Int](list123, _ % 2 != 0)
    l.map(_ * 2).iterator.toSeq shouldBe List(2, 6)
  }
  it should "invoke map with counts correctly" in {
    val l = LazyMonad(list123)
    var count = 0

    def doubleIt(x: Int): Int = { count = count + 1; x * 2 }

    val z = l.map(doubleIt)
    // Now, check that map was truly lazy.
    count shouldBe 0
    z.iterator.toSeq shouldBe list246
    count shouldBe list123.length
  }

  it should "invoke preFilter with counts correctly" in {
    val l = LazyMonad(list123)
    var count = 0

    def isEven(x: Int): Boolean = { count = count + 1; x % 2 == 0 }

    val z = l.preFilter(isEven)
    // Now, check that filter was actually lazy.
    count shouldBe 0
    z.iterator.toSeq shouldBe List(2)
    count shouldBe list123.length
  }

  it should "invoke filter with counts correctly" in {
    val l = LazyMonad(list123)
    var count = 0

    def isEven(x: Int): Boolean = { count = count + 1; x % 2 == 0 }

    val z = l.filter(isEven)
    count shouldBe 0
    z.iterator.toSeq shouldBe List(2)
    count shouldBe list123.length
  }

}
