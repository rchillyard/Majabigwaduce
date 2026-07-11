/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce.core

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import org.slf4j.LoggerFactory

import scala.util.*

/**
 * The purpose of this mapper is to convert a sequence of objects into several sequences, each of which is
 * associated with a key. It must be possible to do further processing (the reduce phase) on each of these
 * resulting sequences independently (and, thus in parallel).
 * Furthermore, the mapping function should try, when possible, to divide the input sequence into a number
 * of more or less equally lengthy sequences.
 *
 * The mapper is an actor whose behavior is parametrized by a function f which converts a (K1,V1) into a (K2,W).
 * It handles a `DoMap` command carrying a `KeyValuePairs[K1,V1]` and a `replyTo` reference.
 * It replies with a `MapperResponse[K2,W]`, which carries both the successfully-mapped results and any
 * exceptions encountered while mapping -- whether that constitutes an overall failure is a decision made
 * by the caller (see Master.doMap), not by the Mapper itself.
 *
 * KeyValuePairs is a convenience wrapper for incoming messages. It has the advantage of not suffering type erasure,
 * and it also has a toString method which simply shows the number of pairs, not their values.
 *
 * Note that logging the actual values received in the incoming message can be VERY verbose.
 * It is therefore recommended practice (if required) to log the values as they pass through the mapper function (f)
 * which is under the control of the application.
 *
 * @author scalaprof
 * @tparam K1 (input) key type (may be Unit)
 * @tparam K2 (output) key type
 * @tparam V1 (input) value type
 * @tparam W  (output) value type
 */
sealed trait MapperCommand[K1, V1, K2, W]

/**
 * Command to map the given key-value pairs, replying to `replyTo` with a `MapperResponse`.
 */
final case class DoMap[K1, V1, K2, W](kvs: KeyValuePairs[K1, V1], replyTo: ActorRef[MapperResponse[K2, W]]) extends MapperCommand[K1, V1, K2, W]

/**
 * Command to stop this Mapper.
 */
final case class CloseMapper[K1, V1, K2, W]() extends MapperCommand[K1, V1, K2, W]

/**
 * The response sent back from a Mapper.
 *
 * @param result     the successfully-mapped key-value pairs, grouped by key.
 * @param exceptions any exceptions encountered while mapping.
 */
case class MapperResponse[K2, W](result: Map[K2, Seq[W]], exceptions: Seq[Throwable])

object Mapper:
  private val logger = LoggerFactory.getLogger("com.phasmid.majabigwaduce.core.Mapper")

  /**
   * @param f function to convert a (K1,V1) pair into a Try[(K2,V2)]
   * @return a Behavior which handles MapperCommand[K1,V1,K2,W].
   */
  def apply[K1, V1, K2, W](f: (K1, V1) => Try[(K2, W)]): Behavior[MapperCommand[K1, V1, K2, W]] =
    MapReduceActor.withLifecycle(logger) { _ =>
      {
        case DoMap(kvs, replyTo) =>
          logger.debug(s"Mapper received $kvs") // NOTE: this only logs the number of elements, not their values.
          // CONSIDER using a form of groupBy to perform this operation
          // NOTE: f is documented to always return a Try, but a misbehaving f (e.g. one that throws
          // during argument unboxing due to an erasure-related ClassCastException) must not be allowed
          // to crash this actor -- Try(...).flatten catches that case too.
          val wk2ts: Seq[Try[(K2, W)]] = for (k1, v1) <- kvs.m yield Try(f(k1, v1)).flatten
          val (result, exceptions) = CleanerCollector.cleanAndCollect(wk2ts)
          replyTo ! MapperResponse(result, exceptions)
          Behaviors.same
        case CloseMapper() =>
          Behaviors.stopped
      }
    }

/**
 * Prior to the Typed migration, this was a distinct actor subclass overriding `isStrict` to retain
 * exceptions instead of failing outright. Now that `MapperResponse` always carries both the
 * successful results and any exceptions, strict-vs-forgiving is purely a decision made by the
 * caller (see Master.doMap / Master.isForgiving), so this is an alias for Mapper -- kept as a
 * separate name for source compatibility with existing call sites.
 */
object Mapper_Forgiving:
  def apply[K1, V1, K2, W](f: (K1, V1) => Try[(K2, W)]): Behavior[MapperCommand[K1, V1, K2, W]] =
    Mapper(f)

/**
 * Case class to package a map of key-value pairs for the purpose of sending to an actor.
 *
 * @param m a map (in sequential form).
 * @tparam K the key type.
 * @tparam V the value type.
 */
case class KeyValuePairs[K, V](m: Seq[(K, V)]):
  override def toString = s"KeyValuePairs: with ${m.size} elements"

object KeyValuePairs:
  def sequence[K, V](vs: Seq[V]): KeyValuePairs[K, V] =
    KeyValuePairs((vs zip LazyList.continually(null.asInstanceOf[K])).map(_.swap))

  // CONSIDER eliminating this
  def map[K, V](vKm: Map[K, V]): KeyValuePairs[K, V] =
    KeyValuePairs(vKm.toSeq)
