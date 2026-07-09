/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce.benchmarks

import akka.actor.ActorSystem
import akka.util.Timeout
import com.phasmid.majabigwaduce.core.*
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.openjdk.jmh.annotations.*

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext}
import scala.util.Random

/**
 * Baseline throughput benchmark for the classic-actor MapReduce pipeline, ahead of the
 * planned migration to typed actors. This mirrors the shape of the CountWords exemplar
 * (a three-stage map/pipe/reduce word count) but replaces CountWords' live HTTP fetches
 * with synthetic in-memory documents, so the benchmark measures actor/map-reduce overhead
 * only, not network variability.
 *
 * Run with: benchmarks/Jmh/run -i 10 -wi 5 -f1 -t1 .*WordCountBenchmark.*
 */
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@Fork(1)
class WordCountBenchmark {

  type Strings = Seq[String]

  // Number of synthetic documents to word-count in each benchmark invocation.
  @Param(Array("10", "100", "1000"))
  var documents: Int = _

  // Number of distinct "servers" (partitions) the documents are grouped under.
  @Param(Array("4"))
  var servers: Int = _

  private var system: ActorSystem = _
  private var docIds: Strings = _
  private var ec: ExecutionContext = _
  private var config: com.typesafe.config.Config = _

  private val timeout: Timeout = Timeout(30.seconds)

  private given stringsZero: Zero[Strings] = new Zero[Strings] {
    def zero: Strings = Nil
  }

  private given intZero: Zero[Int] = new Zero[Int] {
    def zero: Int = 0
  }

  @Setup(Level.Trial)
  def setup(): Unit = {
    system = ActorSystem("WordCountBenchmark")
    ec = system.dispatcher
    val baseConfig = ConfigFactory.load().getConfig("majabigwaduce")
    config = baseConfig.withValue("reducers", ConfigValueFactory.fromAnyRef(servers))
    docIds = (0 until documents).map(i => s"doc-$i")
  }

  @TearDown(Level.Trial)
  def teardown(): Unit = {
    Await.ready(system.terminate(), 30.seconds)
  }

  private def serverFor(docId: String): String = s"server-${Math.abs(docId.hashCode) % servers}"

  private def contentFor(docId: String): String = WordCountBenchmark.syntheticContent(docId)

  private def appendString(a: Strings, v: String): Strings = a :+ v

  private def countFields(gs: Strings): Seq[Int] = for (g <- gs) yield g.split("""\s+""").length

  private def addInts(x: Int, y: Int): Int = x + y

  @Benchmark
  def wordCount(): Int = {
    // A fresh Actors instance per invocation: its actor-name suffix is fixed at construction,
    // so reusing one instance across invocations would collide on actor names since the
    // previous invocation's actors are still alive (see stage1/stage2 close() below).
    given actors: Actors = Actors(system, config)
    given ExecutionContext = ec
    given Timeout = timeout

    val stage1 = MapReduceFirstFold.create({ (w: String) => (serverFor(w), contentFor(w)) }, appendString)(actors, timeout)
    val stage2 = MapReducePipe.create[String, Strings, String, Int, Int](
      (w, gs) => w -> (countFields(gs) reduce addInts),
      addInts,
      1
    )
    val stage3 = Reduce[String, Int, Int](addInts)
    val mr = stage1 & stage2 | stage3
    try Await.result(mr(docIds), timeout.duration)
    finally {
      stage1.close()
      stage2.close()
    }
  }
}

object WordCountBenchmark {
  private val words = Vector("the", "quick", "brown", "fox", "jumps", "over", "lazy", "dog", "scala", "akka", "actor", "mapreduce")

  def syntheticContent(docId: String): String = {
    val r = new Random(docId.hashCode)
    Seq.fill(50)(words(r.nextInt(words.length))).mkString(" ")
  }
}
