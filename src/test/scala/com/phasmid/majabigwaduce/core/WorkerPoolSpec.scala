package com.phasmid.majabigwaduce.core

import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.actor.typed.scaladsl.ActorContext
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpecLike

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import scala.concurrent.duration.*
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

class WorkerPoolSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with should.Matchers {

  private def roundTrip[T](t: T): T =
    val bos = ByteArrayOutputStream()
    val oos = ObjectOutputStream(bos)
    oos.writeObject(t)
    oos.close()
    ObjectInputStream(ByteArrayInputStream(bos.toByteArray)).readObject().asInstanceOf[T]

  private val config: Config = ConfigFactory.load.getConfig("majabigwaduce.Matrix")

  "Worker definitions" must {
    "survive Java serialization, including functions which capture serializable values" in {
      val stop = Set("the")
      val mapper = roundTrip(MapperDef[Unit, String, String, Int]((_, w) => if stop(w) then Failure(new Exception(w)) else Success(w -> 1), forgiving = true))
      mapper.forgiving shouldBe true
      mapper.f((), "fox") shouldBe Success("fox" -> 1)
      mapper.f((), "the").isFailure shouldBe true

      val fold = roundTrip(FoldDef[String, Int, Int](_ + _, () => 0))
      fold.g(fold.z(), 3) shouldBe 3

      val reduce = roundTrip(ReduceDef[String, Int, Int](math.max))
      reduce.g(2, 5) shouldBe 5
    }
  }

  "A Master" must {
    "wait for workers which a pool supplies later" in {
      val promise = Promise[Unit]()
      object LatePool extends WorkerPool:
        def acquire[K1, V1, K2, W, V2](context: ActorContext[?], actors: Actors, mapper: MapperDef[K1, V1, K2, W], nMappers: Int, reducer: ReducerDef[K2, W, V2], nReducers: Int): Future[Workers[K1, V1, K2, W, V2]] =
          val ready = LocalWorkerPool.acquire(context, actors, mapper, nMappers, reducer, nReducers)
          promise.future.flatMap(_ => ready)(using context.executionContext)

      val master = spawn(Master.behavior[String, Int, String, Int, Int](config, MapperDef((k, v) => Success(k -> v), forgiving = false), FoldDef(_ + _, () => 0), _ => LatePool))
      val probe = createTestProbe[Try[Response[String, Int]]]()
      master ! ComputeSeq(Seq("a" -> 1, "a" -> 2, "b" -> 3), probe.ref)
      probe.expectNoMessage(200.millis)
      promise.success(())
      probe.expectMessage(Success(Response(Map(), Map("a" -> 3, "b" -> 3))))
    }

    "reply with the failure when its pool cannot supply workers" in {
      object FailingPool extends WorkerPool:
        def acquire[K1, V1, K2, W, V2](context: ActorContext[?], actors: Actors, mapper: MapperDef[K1, V1, K2, W], nMappers: Int, reducer: ReducerDef[K2, W, V2], nReducers: Int): Future[Workers[K1, V1, K2, W, V2]] =
          Future.failed(MapReduceException("no workers available"))

      val master = spawn(Master.behavior[String, Int, String, Int, Int](config, MapperDef((k, v) => Success(k -> v), forgiving = false), FoldDef(_ + _, () => 0), _ => FailingPool))
      val probe = createTestProbe[Try[Response[String, Int]]]()
      master ! ComputeSeq(Seq("a" -> 1), probe.ref)
      probe.expectMessage(Failure(MapReduceException("no workers available")))
    }
  }

  "WorkerPool.forSystem" must {
    "choose LocalWorkerPool for a non-cluster ActorSystem" in {
      WorkerPool.forSystem(system) shouldBe LocalWorkerPool
    }
  }
}
