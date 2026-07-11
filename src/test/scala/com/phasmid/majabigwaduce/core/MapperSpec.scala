package com.phasmid.majabigwaduce.core

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpecLike

import scala.util.{Failure, Try}

class MapperSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with should.Matchers {

  "A mapper" must {
    "return a MapperResponse with the mapped results and an empty exceptions list" in {
      val f: (String, String) => Try[(Int, String)] = (k, v) => Try(k.hashCode, v.toUpperCase)
      val mapper = spawn(Mapper(f))
      val probe = createTestProbe[MapperResponse[Int, String]]()
      mapper ! DoMap(KeyValuePairs(Seq("hello" -> "Fred", "goodbye" -> "Thursday")), probe.ref)
      probe.expectMessage(MapperResponse(Map(207022353 -> List("THURSDAY"), 99162322 -> List("FRED")), List()))
    }

    "return a MapperResponse with an empty result and the exceptions, when f fails" in {
      val f: (String, String) => Try[(Int, String)] = (_, _) => Failure(MapReduceException("test"))
      val mapper = spawn(Mapper(f))
      val probe = createTestProbe[MapperResponse[Int, String]]()
      mapper ! DoMap(KeyValuePairs(Seq("hello" -> "Fred", "goodbye" -> "Thursday")), probe.ref)
      val response = probe.receiveMessage()
      response.result shouldBe empty
      response.exceptions should have size 2
      response.exceptions.head shouldBe a[MapReduceException]
    }

    "stop upon receiving CloseMapper" in {
      val f: (String, String) => Try[(Int, String)] = (k, v) => Try(k.hashCode, v.toUpperCase)
      val mapper = spawn(Mapper(f))
      mapper ! CloseMapper()
      createTestProbe().expectTerminated(mapper)
    }
  }

  "A forgiving mapper" must {
    // NOTE: prior to the Typed migration, Mapper_Forgiving overrode isStrict to retain exceptions
    // instead of failing outright. Since MapperResponse now always carries both the results and
    // the exceptions, strict-vs-forgiving is purely a decision made by the caller (see
    // Master.doMap), so Mapper_Forgiving is simply an alias for Mapper.
    "behave identically to Mapper" in {
      val f: (String, String) => Try[(Int, String)] = (_, _) => Failure(MapReduceException("test"))
      val mapper = spawn(Mapper_Forgiving(f))
      val probe = createTestProbe[MapperResponse[Int, String]]()
      mapper ! DoMap(KeyValuePairs(Seq("hello" -> "Fred", "goodbye" -> "Thursday")), probe.ref)
      val response = probe.receiveMessage()
      response.result shouldBe empty
      response.exceptions should have size 2
    }
  }
}
