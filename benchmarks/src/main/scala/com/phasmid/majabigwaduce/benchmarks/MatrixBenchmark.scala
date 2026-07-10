/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce.benchmarks

import com.phasmid.majabigwaduce.core.Monoid
import com.phasmid.majabigwaduce.matrix.*
import org.openjdk.jmh.annotations.*

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*
import scala.util.Random

/**
 * Baseline throughput benchmark for Matrix2's actor-based row processing, ahead of the
 * planned migration to typed actors. `Matrix2.forRows` only takes the actor path (via
 * DataDefinition/MapReduce) when `this.size >= cutoff`; `forceActors` toggles between forcing
 * that path unconditionally and forcing the sequential in-thread path, so the same matrix size
 * can be compared both ways -- directly measuring the actor-dispatch overhead in isolation from
 * data volume.
 *
 * NOTE: the number of reducer actors used on the actor path is NOT independently configurable
 * here. DataDefinition's actor context (DDContext) is a JVM-wide singleton, lazily built once
 * from the global application config the first time the DataDefinition object is touched --
 * not something a benchmark instance can override per run. It's whatever
 * `majabigwaduce.DataDefinition.reducers` resolves to at JVM startup (default: 4). This is
 * itself worth keeping in mind for the typed-actors redesign: DataDefinition's reliance on an
 * eagerly-initialized global singleton makes it hard to vary configuration within one JVM.
 *
 * Run with: benchmarks/Jmh/run -i 10 -wi 5 -f1 -t1 .*MatrixBenchmark.*
 */
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@Fork(1)
class MatrixBenchmark {

  // Dimension N of the two NxN matrices being multiplied.
  @Param(Array("5", "20", "50"))
  var size: Int = _

  // true: force the actor-based (DataDefinition/MapReduce) path regardless of size.
  // false: force the sequential in-thread path regardless of size.
  @Param(Array("true", "false"))
  var forceActors: Boolean = _

  private given atMost: Duration = 30.seconds

  private given intProduct: Product[Int] = new Product[Int] {
    def product[X: Numeric, Y: Numeric](x: X, y: Y): Int = summon[Numeric[X]].toInt(x) * summon[Numeric[Y]].toInt(y)
  }

  private given monoidSeqInt: Monoid[Seq[Int]] = new Monoid[Seq[Int]] {
    def combine(x: Seq[Int], y: Seq[Int]): Seq[Int] = x ++ y

    def zero: Seq[Int] = Nil
  }

  private var a: Matrix2[Int] = _
  private var b: Matrix2[Int] = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    val r = new Random(42)

    def randomMatrix(): Matrix2[Int] = Matrix2(Seq.fill(size)(Seq.fill(size)(r.nextInt(10))))

    a = randomMatrix()
    b = randomMatrix()
  }

  @Benchmark
  def multiply(): Matrix[Seq[Int]] = {
    given cutoff: Dimensions = if (forceActors) Dimensions(Seq(1, 1)) else Dimensions(Seq(Int.MaxValue, 1))
    a.product2(b)
  }
}
