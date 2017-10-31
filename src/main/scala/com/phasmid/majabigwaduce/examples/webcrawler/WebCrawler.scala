package com.phasmid.majabigwaduce.examples.webcrawler

import java.net.{URI, URL}

import akka.actor.ActorSystem
import akka.util.Timeout
import com.phasmid.majabigwaduce._
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent._
import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps

/**
  * WebCrawler: an example application of the MapReduce framework.
  * This application is a three-stage map-reduce process (the final stage is a pure reduce process).
  * Stage 1 takes a list of Strings representing URIs, converts to URIs, opens each as a stream, reading the contents and finally returns a map of URI->Seq[String]
  * where the key is the URI of a server, and the Strings are the links retrieved from all the documents read on that server.
  * Stage 2 takes the map of URI->Seq[String] resulting from stage 1 and combines the lists of links.
  * Stage 3 then combines these altogether.
  *
  * This pipeline of stages 1 through 3 is then executed recursively to a given depth. The final output is the list of URLs which have been visited.
  *
  * @author scalaprof
  */
object WebCrawler extends App {
  val configRoot = ConfigFactory.load
  implicit val config: Config = configRoot.getConfig("WebCrawler")
  implicit val system: ActorSystem = ActorSystem(config.getString("name"))
  implicit val timeout: Timeout = getTimeout(config.getString("timeout"))

  import ExecutionContext.Implicits.global

  val ws = if (args.length > 0) args.toSeq else Seq(config.getString("start"))
  private val eventualInt = runWebCrawler(ws, config.getInt("depth"))
  Await.result(eventualInt, 10.minutes)
  for (x <- eventualInt) println(s"total links: $x")

  def runWebCrawler(ws: Seq[String], depth: Int)(implicit system: ActorSystem, config: Config, timeout: Timeout): Future[Int] = {

    def init = Seq[String]()

    val stage1: MapReduce[String, URI, Seq[String]] = MapReduceFirstFold(
      { w: String => val u = new URI(w); (getHostURI(u), u) }, { (a: Seq[String], v: URI) => val s = Source.fromURL(v.toURL).mkString; a :+ s },
      init _
    )
    val stage2 = MapReducePipeFold(
      { (w: URI, gs: Seq[String]) => (w, (for (g <- gs) yield getLinks(w, g)) reduce (_ ++ _)) }, { (a: Seq[String], v: Seq[String]) => a ++ v },
      init _,
      1
    )
    val stage3 = Reduce[Seq[String], Seq[String]](_ ++ _)

    val crawler = stage1 | stage2 | stage3

    def doCrawl(us: Seq[String], all: Seq[String], depth: Int): Future[Seq[String]] =
      if (depth < 0) Future(all)
      else {
        system.log.info(s"doCrawl: depth=$depth; #us=${us.length}; #all=${all.length}")
        val (_, out) = us.partition { u => all.contains(u) }
        for (ws <- crawler(cleanup(out)); gs <- doCrawl(ws.distinct, (all ++ us).distinct, depth - 1)) yield gs
      }

    doCrawl(ws, Seq[String](), depth) transform( { n: Seq[String] => val z = n.length; system.terminate; z }, { x: Throwable => system.log.error(x, "Map/reduce error (typically in map function)"); x })
  }

  private def getLinks(w: URI, g: String): Seq[String] = for (
    nsA <- HTMLParser.parse(g) \\ "a";
    nsH <- nsA \ "@href";
    nH <- nsH.head
  ) yield normalizeURL(w, nH.toString)

  private def cleanup(ws: Seq[String]): Seq[String] = (for (w <- ws; if w.indexOf('?') == -1; t = trim(w, '#')) yield t).distinct

  private def trim(s: String, p: Char): String = {
    val hash = s.indexOf(p)
    if (hash >= 0) s.substring(0, hash) else s
  }

  private def getHostURI(u: URI): URI = new URL(u.getScheme + "://" + u.getHost).toURI

  private def normalizeURL(w: URI, w2: String) = new URL(w.toURL, w2.toString).toString

  // TODO try to combine this with the same method in MapReduceActor
  def getTimeout(t: String) = {
    val durationR = """(\d+)\s*(\w+)""".r
    t match {
      case durationR(n, s) => new Timeout(FiniteDuration(n.toLong, s))
      case _ => Timeout(10 seconds)
    }
  }
}
