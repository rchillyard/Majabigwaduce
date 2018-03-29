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
  type Strings = Seq[String]
  val configRoot = ConfigFactory.load
  implicit val config: Config = configRoot.getConfig("WebCrawler")
  implicit val system: ActorSystem = ActorSystem(config.getString("name"))
  implicit val timeout: Timeout = getTimeout(config.getString("timeout"))

  import ExecutionContext.Implicits.global

  val ws = if (args.length > 0) args.toSeq else Seq(config.getString("start"))
  private val eventualInt = runWebCrawler(ws, config.getInt("depth"))
  Await.result(eventualInt, 10.minutes)
  eventualInt foreach (x => println(s"total links: $x"))

  def runWebCrawler(ws: Strings, depth: Int)(implicit system: ActorSystem, config: Config, timeout: Timeout): Future[Int] = {

    val stage1: MapReduce[String, URI, Strings] = MapReduceFirstFold(getHostAndURI, appendContent, init)
    val stage2: MapReduce[(URI, Strings), URI, Strings] = MapReducePipeFold(getLinkStrings, joinWordLists, init, 1)
    val stage3 = Reduce[URI, Strings, Strings](() => Nil)(_ ++ _)
    val crawler: Strings => Future[Strings] = stage1 & stage2 | stage3

    /**
      * Note that this method is recursive but not tail-recursive
      *
      * @param ws    a sequence of String each representing a URI
      * @param all   the current representation of the result (will be returned if depth < 0)
      * @param depth the depth to which we will recurse
      * @return
      */
    def doCrawl(ws: Strings, all: Strings, depth: Int): Future[Strings] =
      if (depth < 0) Future((all ++ ws).distinct)
      else {
        system.log.info(s"doCrawl: depth=$depth; #ws=${ws.length}; #all=${all.length}")
        system.log.debug(s"doCrawl: ws=$ws; all=$all")
        val (_, out) = ws.partition { u => all.contains(u) }
        system.log.debug(s"doCrawl: out=$out")
        for (ws <- crawler(cleanup(out)); gs <- doCrawl(ws.distinct, (all ++ out).distinct, depth - 1)) yield gs
      }

    doCrawl(ws, Nil, depth) transform( { n => val z = n.length; system.terminate; z }, { x => system.log.error(x, "Map/reduce error (typically in map function)"); x })
  }

  private def init() = Nil: Strings

  private def appendContent(a: Strings, v: URI): Strings = a :+ Source.fromURL(v.toURL).mkString

  private def joinWordLists(a: Strings, v: Strings) = a ++ v

  // Probably, we should get three URIs: the host, the directory and the URI for the string w
  private def getHostAndURI(w: String): (URI, URI) = {
    val u = new URI(w)
    (getHostURI(u), u)
  }

  // We are passing the wrong URL into getLinks: the value of u is the server, not the current directory.
  private def getLinkStrings(u: URI, gs: Strings): (URI, Strings) = (u, (for (g <- gs) yield getLinks(u, g)) reduce (_ ++ _))

  private def getLinks(u: URI, g: String): Strings = for (
    nsA <- HTMLParser.parse(g) \\ "a";
    nsH <- nsA \ "@href";
    nH <- nsH.head
  ) yield normalizeURL(u, nH.toString)

  private def cleanup(ws: Strings): Strings = (for (w <- ws; if w.indexOf('?') == -1; t = trim(w, '#')) yield t).distinct

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
