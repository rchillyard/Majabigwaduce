/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce.benchmarks

import com.phasmid.majabigwaduce.dd.DataDefinition
import org.openjdk.jmh.annotations.*

import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.*
import scala.util.Random

/**
 * Baseline throughput benchmark for DataDefinition's filter/map/reduce pipeline (the
 * RDD-style API), ahead of the planned migration to typed actors. `reducers < 2` runs the
 * pipeline sequentially in-thread; `reducers >= 2` routes evaluation through the actor-based
 * MapReducePipe machinery, with that many reducer actors.
 *
 * NOTE: prior to 2.0.1, `DataDefinition`'s `partitions` argument was silently ignored for
 * reducer-count purposes -- every actor-path evaluation always used the same fixed
 * `majabigwaduce.reducers` config default (4), regardless of what value was passed in. This is
 * now fixed (`LazyDD.evaluate` sizes its Master's reducer pool from `partitions` directly, via a
 * per-call Config overlay -- see `DataDefinition.scala`), so the `reducers` param below now
 * genuinely varies reducer count, which this benchmark demonstrates. See benchmarks/README.md's
 * "Design limitations found via benchmarking" item 3.
 *
 * NOTE: the `pipeline` (a `DataDefinition`, built from `kvs`/`filter`/`map`) is built once per
 * trial, in @Setup, rather than fresh on every @Benchmark invocation -- that wrapper
 * construction was needless per-invocation overhead. This does NOT mean the underlying actors
 * are reused across invocations, though: `pipeline.reduce(...)` still triggers
 * `LazyDD.evaluate()`, which still builds a fresh Master/Mapper/Reducer set on every single
 * call (see benchmarks/README.md's "Design limitations" item 1 -- reusing DataDefinition's
 * actors across separate evaluations is a real correctness hazard and is explicitly deferred).
 *
 * Run with: benchmarks/Jmh/run -i 10 -wi 5 -f1 -t1 .*DataDefinitionBenchmark.*
 */
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@Fork(1)
class DataDefinitionBenchmark {

  // Number of synthetic (key, value) pairs in the pipeline's input -- also the key cardinality,
  // since every key (0 until size) is unique.
  @Param(Array("100", "1000", "10000"))
  var size: Int = _

  // Number of reducer actors Master spins up on the actor path. 1 forces the sequential
  // in-thread path (DataDefinition treats partitions < 2 as "no actors"); values >= 2 route
  // through MapReducePipe with that many reducers.
  @Param(Array("1", "4", "16", "64"))
  var reducers: Int = _

  private var pipeline: DataDefinition[Int, Int] = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    val r = new Random(42)
    val kvs = (0 until size).map(i => i -> r.nextInt(1000))
    val dd: DataDefinition[Int, Int] = DataDefinition(kvs, reducers)
    pipeline = dd
      .filter((kv: (Int, Int)) => kv._2 % 2 == 0)
      .map((kv: (Int, Int)) => (kv._1, kv._2 * 2))
  }

  @TearDown(Level.Trial)
  def teardown(): Unit = {
    DataDefinition.shutdown()
  }

  @Benchmark
  def filterMapReduce(): Int =
    Await.result(pipeline.reduce[Int](_ + _), 30.seconds)
}
