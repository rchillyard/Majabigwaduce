/*
 * Copyright (c) 2026. Phasmid Software
 */

package com.phasmid.majabigwaduce.cluster

import com.phasmid.majabigwaduce.core.*
import org.apache.pekko.actor.typed.receptionist.Receptionist
import org.apache.pekko.actor.typed.scaladsl.ActorContext
import org.apache.pekko.actor.typed.scaladsl.AskPattern.*
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Scheduler}
import org.apache.pekko.pattern.after
import org.apache.pekko.serialization.SerializationExtension
import org.apache.pekko.util.Timeout
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.DurationConverters.*
import scala.util.{Failure, Success, Try}

/**
 * The WorkerPool which a Master uses when its ActorSystem is a cluster member (see WorkerPool.forSystem).
 *
 * The mappers still run locally, on the Master's node. The reducers are spread across the
 * ClusterWorkers found through the Receptionist: the reducer definition (including the reducer
 * function) is sent to each worker, which spawns its share of the reducers and replies with
 * their refs. The Master then drives them exactly as it drives local reducers.
 */
object ClusterWorkerPool extends WorkerPool:
  private val logger = LoggerFactory.getLogger("com.phasmid.majabigwaduce.cluster.ClusterWorkerPool")

  /** The settings under `majabigwaduce.cluster`. */
  final case class Settings(minWorkers: Int, discoveryTimeout: FiniteDuration, spawnTimeout: FiniteDuration)

  object Settings:
    def apply(config: Config): Settings =
      val c = config.getConfig("majabigwaduce.cluster")
      Settings(c.getInt("min-workers"), c.getDuration("discovery-timeout").toScala, c.getDuration("spawn-timeout").toScala)

  def acquire[K1, V1, K2, W, V2](context: ActorContext[?], actors: Actors, mapper: MapperDef[K1, V1, K2, W], nMappers: Int, reducer: ReducerDef[K2, W, V2], nReducers: Int): Future[Workers[K1, V1, K2, W, V2]] =
    val system = context.system
    val settings = Settings(system.settings.config)
    val master: ActorRef[Nothing] = context.self
    given ExecutionContext = context.executionContext
    // NOTE the mappers are spawned here, synchronously, since this is the only point at which we may use context.
    val local: Future[Workers[K1, V1, K2, W, V2]] = LocalWorkerPool.acquire(context, actors, mapper, nMappers, reducer, 0)
    for
      _ <- Future.fromTry(requireJavaSerialization(system))
      _ <- Future.fromTry(checkSerializable(system, reducer))
      workers <- findWorkers(system, settings)
      reducers <- spawnReducers(system, settings, workers, reducer, nReducers, master)
      ws <- local
    yield ws.copy(reducers = reducers)

  /**
   * Fails unless Java serialization is switched on, which a clustered job needs in order to
   * send its reducer function to other nodes.
   */
  def requireJavaSerialization(system: ActorSystem[?]): Try[Unit] =
    if system.settings.config.getBoolean("pekko.actor.allow-java-serialization") then Success(())
    else Failure(MapReduceException(
      "a clustered job sends its reducer function to other nodes using Java serialization, which is switched off: " +
        "set pekko.actor.allow-java-serialization = on (only on a trusted network -- see doc/Clustering.md)"))

  /**
   * Checks that `reducer` can be serialized, before anything is sent. Without this check, Pekko
   * would just log the failure and drop the message, and the job would time out.
   */
  private[cluster] def checkSerializable(system: ActorSystem[?], reducer: ReducerDef[?, ?, ?]): Try[Unit] =
    SerializationExtension(system).serialize(reducer).map(_ => ()).recoverWith {
      case x => Failure(MapReduceException(
        s"the reducer function can't be sent to other nodes because it is not serializable (${x.getMessage}). " +
          "A function sent to a cluster must capture only serializable values: a common cause is calling a method " +
          "of an enclosing class (which captures `this`) that holds an ActorSystem, a logger or similar", x))
    }

  /** Finds at least `minWorkers` ClusterWorkers, waiting up to `discoveryTimeout` for them to appear. */
  private def findWorkers(system: ActorSystem[?], settings: Settings)(using ExecutionContext): Future[Seq[ActorRef[WorkerCommand]]] =
    given Timeout = Timeout(settings.discoveryTimeout)
    given Scheduler = system.scheduler
    val deadline = settings.discoveryTimeout.fromNow

    def attempt(): Future[Seq[ActorRef[WorkerCommand]]] =
      system.receptionist.ask[Receptionist.Listing](Receptionist.Find(ClusterWorker.key, _)).flatMap { listing =>
        val workers = listing.serviceInstances(ClusterWorker.key).toSeq.sortBy(_.path.toString)
        if workers.size >= settings.minWorkers then
          logger.debug(s"found ${workers.size} workers: ${workers.map(_.path.address).mkString(", ")}")
          Future.successful(workers)
        else if deadline.isOverdue() then
          Future.failed(MapReduceException(s"found ${workers.size} majabigwaduce worker(s) within ${settings.discoveryTimeout}, but min-workers is ${settings.minWorkers}: has ClusterWorker.start been called on the worker nodes?"))
        else after(200.millis)(attempt())(using system)
      }

    attempt()

  /**
   * Asks each worker to spawn its share of `nReducers` reducers, and returns all their refs,
   * interleaved by worker so that the Master's round-robin distribution alternates between nodes.
   */
  private def spawnReducers[K2, W, V2](system: ActorSystem[?], settings: Settings, workers: Seq[ActorRef[WorkerCommand]], reducer: ReducerDef[K2, W, V2], nReducers: Int, master: ActorRef[Nothing])(using ExecutionContext): Future[Seq[ActorRef[ReducerCommand[K2, W, V2]]]] =
    given Timeout = Timeout(settings.spawnTimeout)
    given Scheduler = system.scheduler
    val shares = ClusterWorkerPool.shares(math.max(nReducers, 1), workers.size)
    val spawned: Seq[Future[Seq[ActorRef[ReducerCommand[K2, W, V2]]]]] =
      for (worker, n) <- workers.zip(shares) if n > 0 yield
        worker.ask[ReducersSpawned[K2, W, V2]](SpawnReducers(reducer, n, master, _)).map(_.reducers)
    Future.sequence(spawned).map(interleave)

  /** Splits `n` items as evenly as possible into `k` shares, the larger shares first. */
  private[cluster] def shares(n: Int, k: Int): Seq[Int] =
    for i <- 0 until k yield n / k + (if i < n % k then 1 else 0)

  /** Interleaves the sequences: the first of each, then the second of each, and so on. */
  private[cluster] def interleave[X](xss: Seq[Seq[X]]): Seq[X] =
    if xss.forall(_.isEmpty) then Nil
    else xss.flatMap(_.headOption) ++ interleave(xss.map(_.drop(1)))
