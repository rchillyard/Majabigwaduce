/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce.benchmarks

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.util.Timeout
import com.phasmid.majabigwaduce.core.*
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.openjdk.jmh.annotations.*

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext}
import scala.util.Try

/**
 * Demonstrates map-side parallelism (`Master.scala` spawning `mappers` mapper actors and fanning
 * the input batch out across them) with a deliberately CPU-heavy mapper function `f`. None of
 * this suite's other benchmarks have a mapper function expensive enough to show a signal above
 * ask-overhead/message-passing noise -- `WordCountBenchmark`'s stage-2 batch never grows past
 * `keyCardinality`, and `MatrixBenchmark`/`DataDefinitionBenchmark`'s mapper functions are cheap
 * int arithmetic. See benchmarks/README.md's "Design limitations found via benchmarking" item 2.
 *
 * Uses `MapReducePipe` directly (a single map-reduce stage, bypassing `DataDefinition`) so the
 * `mappers` config key can be set explicitly per invocation -- `DataDefinitionBenchmark` and
 * `MatrixBenchmark` route through a shared, JVM-wide `DDContext`, which doesn't support this.
 *
 * The reducer function here is a trivial pass-through: every input key is unique, so each
 * reduce-side group is a singleton sequence, and `reduceLeft` never actually invokes it. This
 * isolates the measurement to the map phase, uncontaminated by reduce-side cost.
 *
 * Run with: benchmarks/Jmh/run -i 10 -wi 5 -f1 .*MapperParallelismBenchmark.*
 */
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@Fork(1)
class MapperParallelismBenchmark {

  // Number of synthetic (key, value) pairs in the batch.
  @Param(Array("400"))
  var items: Int = _

  // Number of mapper actors Master spins up to parallelize the map phase.
  @Param(Array("1", "4"))
  var mappers: Int = _

  private var system: ActorSystem[Nothing] = _
  private var ec: ExecutionContext = _
  private var kvs: Seq[(Int, Int)] = _
  private var config: Config = _

  private given timeout: Timeout = Timeout(30.seconds)

  @Setup(Level.Trial)
  def setup(): Unit = {
    system = ActorSystem(Behaviors.empty, "MapperParallelismBenchmark")
    ec = system.executionContext
    kvs = (0 until items).map(i => i -> i)
    val baseConfig = ConfigFactory.load().getConfig("majabigwaduce")
    config = baseConfig.withValue("mappers", ConfigValueFactory.fromAnyRef(mappers))
  }

  @TearDown(Level.Trial)
  def teardown(): Unit = {
    system.terminate()
    Await.ready(system.whenTerminated, 30.seconds)
  }

  // A deliberately expensive per-element computation -- a fixed-iteration hash-mixing loop --
  // so total sequential cost is measurable (tens to hundreds of ms) rather than dominated by
  // JIT/dispatch noise.
  private def expensiveHash(k: Int, v: Int): Try[(Int, Int)] = Try {
    var h = k * 31 + v
    var i = 0
    while i < 200000 do
      h = (h * 2654435761L).toInt ^ (h >>> 15)
      i += 1
    k -> h
  }

  @Benchmark
  def mapHeavyWorkload(): Map[Int, Int] = {
    given actors: Actors = Actors(system, config)
    given ExecutionContext = ec

    val stage = MapReducePipe[Int, Int, Int, Int, Int](expensiveHash, (_, w) => w, 1)
    try Await.result(stage(kvs), timeout.duration)
    finally stage.close()
  }
}
