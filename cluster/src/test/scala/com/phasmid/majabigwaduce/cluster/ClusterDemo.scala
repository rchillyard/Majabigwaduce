/*
 * Copyright (c) 2026. Phasmid Software
 */

package com.phasmid.majabigwaduce.cluster

import com.phasmid.majabigwaduce.core.Actors
import com.phasmid.majabigwaduce.examples.countwords.{CountWords, ResourceHttpClient}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.typed.Cluster
import org.apache.pekko.util.Timeout
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext}
import scala.io.StdIn
import scala.util.{Failure, Success, Try}

/**
 * A two-terminal demo of CountWords with its reducers on another node (see doc/Clustering.md).
 *
 * In the first terminal, start the worker node (the cluster's seed node):
 * {{{
 *   sbt "cluster/Test/runMain com.phasmid.majabigwaduce.cluster.clusterWorkerNode"
 * }}}
 * In the second terminal, once the worker is up, run CountWords on the master node:
 * {{{
 *   sbt "cluster/Test/runMain com.phasmid.majabigwaduce.cluster.clusterCountWords"
 * }}}
 * The worker's log shows it spawning the job's reducers, and stopping them when the job ends.
 */
object ClusterDemo:
  val systemName = "CountWordsCluster"
  val workerPort = 2551
  val masterPort = 2552

  def config(port: Int): Config =
    ConfigFactory.parseString(s"pekko.remote.artery.canonical.port = $port")
      .withFallback(ConfigFactory.load("cluster-demo"))

  def startNode(port: Int): ActorSystem[Nothing] =
    val system = ActorSystem[Nothing](Behaviors.empty[Nothing], systemName, config(port))
    println(s"node ${Cluster(system).selfMember.address} is joining the cluster")
    system

/** Starts a worker node, which runs reducers for any Master in the cluster, until RETURN is pressed. */
@main def clusterWorkerNode(args: String*): Unit =
  val port = args.headOption.map(_.toInt).getOrElse(ClusterDemo.workerPort)
  val system = ClusterDemo.startNode(port)
  ClusterWorker.start(system)
  println("worker started: press RETURN to stop it")
  StdIn.readLine()
  system.terminate()
  Await.ready(system.whenTerminated, 30.seconds)

/**
 * Runs CountWords on a master node, whose reducers run on the worker node(s), then leaves the cluster.
 * The arguments are an optional port, followed by the URLs of the documents to count (they are read from
 * the test resources, as in the CountWords specs).
 */
@main def clusterCountWords(args: String*): Unit =
  val (port, urls) = args.headOption.flatMap(_.toIntOption) match
    case Some(p) => p -> args.tail
    case None => ClusterDemo.masterPort -> args
  given system: ActorSystem[Nothing] = ClusterDemo.startNode(port)
  given config: Config = system.settings.config.getConfig("majabigwaduce.CountWords")
  given timeout: Timeout = Actors.getTimeout(config.getString("timeout"))
  given logger: Logger = LoggerFactory.getLogger(classOf[CountWords])
  given ec: ExecutionContext = system.executionContext
  val ws = if urls.nonEmpty then urls else Seq("https://www.bbc.com/doc1", "https://www.bbc.com/doc2", "https://www.cnn.com/doc3")
  val cw = CountWords(new ResourceHttpClient("/countwords").getResource)
  val result = Try(Await.result(cw(ws), 30.seconds))
  cw.close()
  result match
    case Success(n) => println(s"Word count = $n")
    case Failure(x) => println(s"Failure: ${x.getMessage}")
  system.terminate()
  Await.ready(system.whenTerminated, 30.seconds)
