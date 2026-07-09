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
 * RDD-style API), ahead of the planned migration to typed actors. `partitions < 2` runs the
 * pipeline sequentially in-thread; `partitions >= 2` routes evaluation through the actor-based
 * MapReducePipe machinery -- `forceActors` toggles between the two so the same data size can
 * be compared both ways.
 *
 * NOTE: as with MatrixBenchmark, the number of reducer actors used on the actor path isn't
 * independently configurable here -- DataDefinition's actor context (DDContext) is a JVM-wide
 * singleton built once, lazily, from the global application config the first time the
 * DataDefinition object is touched. See MatrixBenchmark's doc comment for the full
 * explanation; the same forced-exit delay per fork applies here too.
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

  // Number of synthetic (key, value) pairs in the pipeline's input.
  @Param(Array("100", "1000", "10000"))
  var size: Int = _

  // true: force the actor-based (MapReducePipe) evaluation path (partitions = 4).
  // false: force the sequential in-thread path (partitions = 1).
  @Param(Array("true", "false"))
  var forceActors: Boolean = _

  private var kvs: Seq[(Int, Int)] = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    val r = new Random(42)
    kvs = (0 until size).map(i => i -> r.nextInt(1000))
  }

  @Benchmark
  def filterMapReduce(): Int = {
    val partitions = if (forceActors) 4 else 1
    val dd: DataDefinition[Int, Int] = DataDefinition(kvs, partitions)
    val pipeline = dd
      .filter((kv: (Int, Int)) => kv._2 % 2 == 0)
      .map((kv: (Int, Int)) => (kv._1, kv._2 * 2))
    Await.result(pipeline.reduce[Int](_ + _), 30.seconds)
  }
}
