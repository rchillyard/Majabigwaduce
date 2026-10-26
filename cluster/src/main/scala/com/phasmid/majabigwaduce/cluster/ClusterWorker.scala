/*
 * Copyright (c) 2026. Phasmid Software
 */

package com.phasmid.majabigwaduce.cluster

import com.phasmid.majabigwaduce.core.{Master, ReducerCommand, ReducerDef}
import org.apache.pekko.actor.typed.receptionist.{Receptionist, ServiceKey}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior, Terminated}
import org.slf4j.LoggerFactory

/**
 * The command protocol of a ClusterWorker.
 * NOTE these messages travel between nodes, using Java serialization (see doc/Clustering.md).
 */
sealed trait WorkerCommand extends Serializable

/**
 * Asks a worker to spawn `n` reducers built from `reducer`, for the job run by `master`.
 * The reducers are stopped when `master` stops (or its node leaves the cluster).
 *
 * @param reducer the definition of the reducers (including the reducer function itself).
 * @param n       the number of reducers to spawn.
 * @param master  the Master actor which will use the reducers.
 * @param replyTo where to send the refs of the new reducers.
 */
final case class SpawnReducers[K2, W, V2](reducer: ReducerDef[K2, W, V2], n: Int, master: ActorRef[Nothing], replyTo: ActorRef[ReducersSpawned[K2, W, V2]]) extends WorkerCommand:
  private[cluster] def jobBehavior: Behavior[Unit] = ClusterWorker.job(this)

/** The reply to SpawnReducers. */
final case class ReducersSpawned[K2, W, V2](reducers: Seq[ActorRef[ReducerCommand[K2, W, V2]]])

/**
 * A ClusterWorker offers reducers to the Masters of a cluster. Start one (with `ClusterWorker.start`)
 * on every node which should run reducers -- including, if you like, the node which runs the Master.
 * It registers itself with the Receptionist, so that a ClusterWorkerPool can find it.
 */
object ClusterWorker:
  private val logger = LoggerFactory.getLogger("com.phasmid.majabigwaduce.cluster.ClusterWorker")

  /** The key under which workers register with the Receptionist. */
  val key: ServiceKey[WorkerCommand] = ServiceKey[WorkerCommand]("majabigwaduce-worker")

  def apply(): Behavior[WorkerCommand] = Behaviors.setup { context =>
    context.system.receptionist ! Receptionist.Register(key, context.self)
    logger.debug(s"worker registered at ${context.self.path}")
    Behaviors.receiveMessage {
      case s: SpawnReducers[?, ?, ?] =>
        context.spawnAnonymous(s.jobBehavior)
        Behaviors.same
    }
  }

  /**
   * Starts a ClusterWorker in `system`.
   *
   * @param system a cluster member, with `pekko.actor.allow-java-serialization = on`.
   * @return the worker.
   */
  def start(system: ActorSystem[?]): ActorRef[WorkerCommand] =
    ClusterWorkerPool.requireJavaSerialization(system).get
    system.systemActorOf(ClusterWorker(), "majabigwaduce-worker")

  /**
   * The actor which looks after one job's reducers on this node: it spawns them as its children,
   * replies with their refs, and stops (taking its reducers with it) when the job's Master stops.
   */
  private[cluster] def job[K2, W, V2](s: SpawnReducers[K2, W, V2]): Behavior[Unit] = Behaviors.setup { context =>
    val behavior = s.reducer.behavior
    val reducers = for i <- 1 to s.n yield context.spawn(behavior, s"${Master.sReducer}-$i")
    context.watch(s.master)
    logger.debug(s"spawned ${s.n} reducers for ${s.master.path}")
    s.replyTo ! ReducersSpawned(reducers)
    Behaviors.receiveSignal {
      case (_, Terminated(_)) =>
        logger.debug(s"master ${s.master.path} has stopped: stopping its reducers")
        Behaviors.stopped
    }
  }
