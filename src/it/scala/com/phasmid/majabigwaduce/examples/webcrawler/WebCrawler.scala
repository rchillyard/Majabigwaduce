/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce.examples.webcrawler

import akka.actor.ActorSystem
import akka.util.Timeout
import com.phasmid.majabigwaduce.core._
import com.typesafe.config.{Config, ConfigFactory}

import java.net.{URI, URL}
import scala.concurrent._
import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps
import scala.util.{Failure, Success, Try, Using}

/**
 * WebCrawler: an example application of the MapReduce framework.
 * This application is a three-stage map-reduce process (the final stage is a pure reduce process).
 * Stage 1 takes a list of Strings representing URIs, converts to URIs, opens each as a stream, reading the contents and finally returns a map of URI->Seq[String]
 * where the key is the URI of a server, and the Strings are the links retrieved from all the documents read on that server.
 * Stage 2 takes the map of URI->Seq[String] resulting from stage 1 and combines the lists of links.
 * Stage 3 then combines these altogether.
 *
 * TODO this web crawler is a depth-first algorithm -- change it to breadth-first
 *
 * This pipeline of stages 1 through 3 is then executed recursively to a given depth. The final output is the list of URLs which have been visited.
 *
 * @author scalaprof
 */
case class WebCrawler(depth: Int)(implicit system: ActorSystem, config: Config, timeout: Timeout, ec: ExecutionContext) extends (Strings => Future[Int]) {

  trait StringsZero$ extends Zero[Strings] {
    def zero: Strings = Nil: Strings
  }

  val actors: Actors = Actors(implicitly[ActorSystem], implicitly[Config])

  implicit object StringsZero$ extends StringsZero$

  val g: (Strings, URI) => Strings = (ws, u) => appendContent(ws, u) match {
    case Success(w) => w
    case Failure(x) => actors.logException(s"problem with reduce function: ${x.getLocalizedMessage}")
      Nil
  }

  val stage1: MapReduce[String, URI, Strings] = MapReduceFirstFold(getHostAndURI, g)(actors, timeout)
  val stage2: MapReduce[(URI, Strings), URI, Strings] = MapReducePipeFold.create(getLinkStrings, joinWordLists, 1)(actors, timeout)
  val stage3: Reduce[URI, Strings, Strings] = Reduce[URI, Strings, Strings](_ ++ _)
  val crawler: Strings => Future[Strings] = stage1 & stage2 | stage3

  override def apply(ws: Strings): Future[Int] = doCrawl(ws, Nil, depth) transform( { n => val z = n.length; system.terminate(); z }, { x => system.log.error(x, "Map/reduce error (typically in map function)"); x })

  private def doCrawl(ws: Strings, all: Strings, depth: Int): Future[Strings] =
    if (depth < 0) Future((all ++ ws).distinct)
    else {
      def cleanup(ws: Strings): Strings = (for (w <- ws; if w.indexOf('?') == -1; t = trim(w, '#')) yield t).distinct

      def trim(s: String, p: Char): String = {
        val hash = s.indexOf(p)
        if (hash >= 0) s.substring(0, hash) else s
      }

      system.log.info(s"doCrawl: depth=$depth; #ws=${ws.length}; #all=${all.length}")
      system.log.debug(s"doCrawl: ws=$ws; all=$all")
      val (_, out) = ws.partition { u => all.contains(u) }
      system.log.debug(s"doCrawl: out=$out")
      for (ws <- crawler(cleanup(out)); gs <- doCrawl(ws.distinct, (all ++ out).distinct, depth - 1)) yield gs
    }

  // Probably, we should get three URIs: the host, the directory and the URI for the string w
  private def getHostAndURI(w: String): Try[(URI, URI)] = {
    def getHostURI(u: URI): URI = new URL(u.getScheme + "://" + u.getHost).toURI

    Try {
      val u = new URI(w)
      (getHostURI(u), u)
    }
  }

  // TODO use Using...
  //  private def appendContent(a: Strings, v: URI): Strings = a :+ Source.fromURL(v.toURL).mkString

  private def appendContent(a: Strings, v: URI): Try[Strings] = Using(Source.fromURL(v.toURL)) { s => a :+ s.mkString }

  // We are passing the wrong URL into getLinks: the value of u is the server, not the current directory.
  private def getLinkStrings(u: URI, gs: Strings): (URI, Strings) = {
    def normalizeURL(w: URI, w2: String) = new URL(w.toURL, w2).toString

    def getLinks(u: URI, g: String): Strings = for (
      nsA <- HTMLParser.parse(g) \\ "a";
      nsH <- nsA \ "@href";
      nH <- nsH.headOption.toSeq
    ) yield normalizeURL(u, nH.toString)

    (u, (for (g <- gs) yield getLinks(u, g)) reduce (_ ++ _))
  }

  private def joinWordLists(a: Strings, v: Strings) = a ++ v
}


object WebCrawler extends App {

  implicit val config: Config = ConfigFactory.load.getConfig("WebCrawler")
  implicit val system: ActorSystem = ActorSystem(config.getString("name"))
  implicit val timeout: Timeout = getTimeout(config.getString("timeout"))

  import ExecutionContext.Implicits.global

  val ws = if (args.length > 0) args.toSeq else Seq(config.getString("start"))
  val crawler = WebCrawler(config.getInt("depth"))
  private val xf = crawler(ws)
  xf foreach (x => println(s"total links: $x"))
  Await.ready(xf, 10.minutes)

  // TODO try to combine this with the same method in MapReduceActor
  def getTimeout(t: String) = {
    val durationR = """(\d+)\s*(\w+)""".r
    t match {
      case durationR(n, s) => new Timeout(FiniteDuration(n.toLong, s))
      case _ => Timeout(10 seconds)
    }
  }
}
