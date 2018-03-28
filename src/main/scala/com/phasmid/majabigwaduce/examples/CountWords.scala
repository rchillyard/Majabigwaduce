package com.phasmid.majabigwaduce.examples

import java.net.URI

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.util.Timeout
import com.phasmid.majabigwaduce._
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps

trait HttpClient {
  def getResource(w: String): Resource
}

trait Resource {
  def getServer: URI

  def getContent: String
}

case class CountWords(resourceFunc: String => Resource)(implicit system: ActorSystem, logger: LoggingAdapter, config: Config, timeout: Timeout, ec: ExecutionContext) extends (Seq[String] => Future[Int]) {
  override def apply(v1: Seq[String]): Future[Int] = {

    val stage1: MapReduce[String, URI, Seq[String]] = MapReduceFirstFold(
      { w: String => val u = resourceFunc(w); logger.debug(s"stage1 map: $w"); (u.getServer, u.getContent) }, appendString,
      init _
    )

    val stage2: MapReduce[(URI, Seq[String]), URI, Int] = MapReducePipe(
      (w, gs) => w -> (countFields(gs) reduce addInts),
      addInts,
      1
    )
    val stage3 = Reduce[URI, Int, Int](() => 0)(addInts)
    val mr = stage1 & stage2 | stage3
    mr(v1)
  }

  private def countFields(gs: Seq[String]) = for (g <- gs) yield g.split("""\s+""").length

  private def init = Seq[String]()

  private def addInts(x: Int, y: Int) = x + y

  private def appendString(a: Seq[String], v: String) = a :+ v
}

/**
  * CountWords: an example application of the MapReduce framework.
  * This application is a three-stage map-reduce process (the final stage is a pure reduce process).
  * Stage 1 takes a list of Strings representing URIs, converts to URIs, opens each as a stream, reading the contents and finally returns a map of URI->Seq[String]
  * where the key is the URI of a server, and the Strings are the contents of each of the documents retrieved from that server.
  * Stage 2 takes the map of URI->Seq[String] resulting from stage 1 and adds the lengths of the documents (in words) to each other. The final result is a map of
  * URI->Int where the value is the total number of words read from the server represented by the key.
  * Stage 3 then sums these values together to yield a grand total.
  *
  * @author scalaprof
  */
object CountWords {
  def apply(hc: HttpClient, args: Array[String]): Future[Int] = {
    val configRoot = ConfigFactory.load
    implicit val config: Config = configRoot.getConfig("CountWords")
    implicit val system: ActorSystem = ActorSystem(config.getString("name"))
    implicit val timeout: Timeout = getTimeout(config.getString("timeout"))
    implicit val logger: LoggingAdapter = system.log
    import ExecutionContext.Implicits.global
    import Init._

    val ws = if (args.length > 0) args.toSeq else Seq("http://www.bbc.com/doc1", "http://www.cnn.com/doc2", "http://default/doc3", "http://www.bbc.com/doc2", "http://www.bbc.com/doc3")
    CountWords(hc.getResource).apply(ws)
  }

  // TODO try to combine this with the same method in MapReduceActor
  def getTimeout(t: String): Timeout = {
    val durationR = """(\d+)\s*(\w+)""".r
    t match {
      case durationR(n, s) => new Timeout(FiniteDuration(n.toLong, s))
      case _ => Timeout(10 seconds)
    }
  }
}
