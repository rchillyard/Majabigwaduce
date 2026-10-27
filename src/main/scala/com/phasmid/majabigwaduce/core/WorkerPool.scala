/*
 * Copyright (c) 2026. Phasmid Software
 */

package com.phasmid.majabigwaduce.core

import org.apache.pekko.actor.typed.scaladsl.ActorContext
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior}
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.util.Try

/**
 * Describes how to build a mapper behavior. Unlike a `Behavior`, a definition holds only the mapper
 * function and its mode, and is `Serializable` -- so a non-local WorkerPool can ship it to
 * another node and build the behavior there (see doc/Clustering.md).
 *
 * @param f         the mapper function.
 * @param forgiving whether to use Mapper_Forgiving rather than Mapper.
 */
final case class MapperDef[K1, V1, K2, W](f: (K1, V1) => Try[(K2, W)], forgiving: Boolean) extends Serializable:
  def behavior: Behavior[MapperCommand[K1, V1, K2, W]] = if forgiving then Mapper_Forgiving(f) else Mapper(f)

/**
 * Describes how to build a reducer behavior: either a plain Reducer or a Reducer_Fold.
 * Like MapperDef, it is `Serializable` so that it can be shipped to another node.
 */
sealed trait ReducerDef[K2, W, V2] extends Serializable:
  def behavior: Behavior[ReducerCommand[K2, W, V2]]

/** Definition of a plain Reducer (reduceLeft with `g`). */
final case class ReduceDef[K2, W, V2 >: W](g: (V2, W) => V2) extends ReducerDef[K2, W, V2]:
  def behavior: Behavior[ReducerCommand[K2, W, V2]] = Reducer(g)

/**
 * Definition of a Reducer_Fold (foldLeft with `g` from `z()`).
 * NOTE `z` is kept as a function so that each reduction still starts from a fresh zero,
 * exactly as Reducer_Fold's by-name parameter does.
 */
final case class FoldDef[K2, W, V2](g: (V2, W) => V2, z: () => V2) extends ReducerDef[K2, W, V2]:
  def behavior: Behavior[ReducerCommand[K2, W, V2]] = Reducer_Fold(g, z())

/** The mapper and reducer actors which a Master drives. */
final case class Workers[K1, V1, K2, W, V2](mappers: Seq[ActorRef[MapperCommand[K1, V1, K2, W]]], reducers: Seq[ActorRef[ReducerCommand[K2, W, V2]]])

/**
 * Where a Master's mappers and reducers come from.
 *
 * `acquire` is called once, from the Master's setup (i.e. on the Master's own thread, so an
 * implementation may use `context` directly). It returns a Future so that an implementation can
 * obtain workers asynchronously -- e.g. by asking remote nodes to spawn them.
 */
trait WorkerPool:
  def acquire[K1, V1, K2, W, V2](context: ActorContext[?], actors: Actors, mapper: MapperDef[K1, V1, K2, W], nMappers: Int, reducer: ReducerDef[K2, W, V2], nReducers: Int): Future[Workers[K1, V1, K2, W, V2]]

object WorkerPool:
  private val logger = LoggerFactory.getLogger(classOf[WorkerPool])

  /** The configuration key which a clustering module uses to name its WorkerPool object. */
  val clusterPoolKey = "majabigwaduce.cluster.worker-pool"

  /**
   * Chooses the WorkerPool for a Master running in `system`: the pool named by
   * `majabigwaduce.cluster.worker-pool` if `system` is a cluster member (and that key is set,
   * i.e. the majabigwaduce-cluster module is on the classpath); otherwise LocalWorkerPool.
   *
   * @param system the ActorSystem in which the Master runs.
   * @return the WorkerPool to use.
   */
  def forSystem(system: ActorSystem[?]): WorkerPool =
    val config = system.settings.config
    val provider = config.getString("pekko.actor.provider")
    val isCluster = provider == "cluster" || provider.endsWith(".ClusterActorRefProvider")
    if isCluster && config.hasPath(clusterPoolKey) then
      val name = config.getString(clusterPoolKey)
      logger.debug(s"using cluster worker pool: $name")
      load(name)
    else
      if isCluster then logger.warn(s"$system is a cluster member but $clusterPoolKey is not set (is majabigwaduce-cluster on the classpath?): workers will be local")
      LocalWorkerPool

  // NOTE a Scala object compiles to a class named with a trailing '$', whose instance is in MODULE$.
  private def load(name: String): WorkerPool =
    Class.forName(name + "$").getField("MODULE$").get(null).asInstanceOf[WorkerPool]

/**
 * The default WorkerPool: spawns the workers as local children of the Master, so they are
 * stopped when the Master stops. This is what Master did directly before 2.1.0.
 */
object LocalWorkerPool extends WorkerPool:
  private val logger = LoggerFactory.getLogger("com.phasmid.majabigwaduce.core.Master")

  def acquire[K1, V1, K2, W, V2](context: ActorContext[?], actors: Actors, mapper: MapperDef[K1, V1, K2, W], nMappers: Int, reducer: ReducerDef[K2, W, V2], nReducers: Int): Future[Workers[K1, V1, K2, W, V2]] =
    logger.debug(s"creating $nMappers mappers")
    val mapperBehavior = mapper.behavior
    val mappers =
      for i <- 1 to nMappers yield
        actors.createActor[MapperCommand[K1, V1, K2, W]]((b, n) => context.spawn(b, n), Some(s"${Master.sMpr}-$i"), mapperBehavior)
    logger.debug(s"creating $nReducers reducers")
    val reducerBehavior = reducer.behavior
    val reducers =
      for i <- 1 to nReducers yield
        actors.createActor[ReducerCommand[K2, W, V2]]((b, n) => context.spawn(b, n), Some(s"${Master.sReducer}-$i"), reducerBehavior)
    Future.successful(Workers(mappers, reducers))
