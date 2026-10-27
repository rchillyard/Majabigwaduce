package com.phasmid.majabigwaduce.cluster

import com.phasmid.majabigwaduce.core.*
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem}
import org.apache.pekko.cluster.MemberStatus
import org.apache.pekko.cluster.typed.Cluster

import java.net.ServerSocket
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future, Promise}
import scala.util.Success

/**
 * Helpers for tests which run several cluster nodes (ActorSystems) in one JVM, joined over
 * Artery TCP on 127.0.0.1.
 */
object TestCluster:
  val systemName = "MajabigwaduceClusterTest"

  def freePort(): Int =
    val socket = new ServerSocket(0)
    try socket.getLocalPort finally socket.close()

  def config(port: Int, seedPort: Int, extra: String = ""): Config = ConfigFactory.parseString(
    s"""
       |pekko.actor.provider = cluster
       |pekko.actor.allow-java-serialization = on
       |pekko.actor.warn-about-java-serializer-usage = off
       |pekko.remote.artery.canonical { hostname = "127.0.0.1", port = $port }
       |pekko.cluster.seed-nodes = ["pekko://$systemName@127.0.0.1:$seedPort"]
       |pekko.cluster.jmx.multi-mbeans-in-same-jvm = on
       |$extra
       |""".stripMargin).withFallback(ConfigFactory.load())

  /** Starts `n` nodes, the first being the seed node, and waits until they are all Up. */
  def start(n: Int, extra: String = ""): Seq[ActorSystem[Nothing]] =
    val ports = Seq.fill(n)(freePort())
    val systems = ports.map(p => ActorSystem[Nothing](Behaviors.empty[Nothing], systemName, config(p, ports.head, extra)))
    val deadline = 30.seconds.fromNow
    while !systems.forall(s => Cluster(s).state.members.count(_.status == MemberStatus.Up) == n) do
      if deadline.isOverdue() then throw new AssertionError(s"cluster of $n nodes did not come up")
      Thread.sleep(100)
    systems

  def stop(systems: Seq[ActorSystem[?]]): Unit =
    systems.reverse.foreach(_.terminate())
    systems.foreach(s => Await.ready(s.whenTerminated, 30.seconds))

  val jobConfig: Config = ConfigFactory.load.getConfig("majabigwaduce.CountWords")

  /**
   * Acquires workers from ClusterWorkerPool on behalf of a stand-in "master" actor in `system`.
   * Sending the returned actor a message stops it, as a Master would stop at the end of a job.
   */
  def acquire[K2, W, V2](system: ActorSystem[?], reducer: ReducerDef[K2, W, V2], nReducers: Int): (ActorRef[Unit], Future[Workers[String, String, K2, W, V2]]) =
    val promise = Promise[Workers[String, String, K2, W, V2]]()
    val mapper = MapperDef[String, String, K2, W]((_, _) => scala.util.Failure(new UnsupportedOperationException), forgiving = true)
    val master = system.systemActorOf(Behaviors.setup[Unit] { context =>
      promise.completeWith(ClusterWorkerPool.acquire(context, Actors(context.system, jobConfig), mapper, 1, reducer, nReducers))
      Behaviors.receiveMessage(_ => Behaviors.stopped)
    }, s"test-master-${Actors.getCount}")
    master -> promise.future

  def address(system: ActorSystem[?]) = Cluster(system).selfMember.address
