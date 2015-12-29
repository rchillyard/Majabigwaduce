package com.phasmid.majabigwaduce.examples.webcrawler

import scala.util._
import scala.concurrent._
import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.util.Timeout
import akka.util.Timeout._
import java.net.URI
import com.phasmid.majabigwaduce._
import scala.io.Source
import java.net.URL

/**
 * WebCrawler: an example application of the MapReduce framework.
 * NOT YET READY!
 * This application is a three-stage map-reduce process (the final stage is a pure reduce process).
 * Stage 1 takes a list of Strings representing URIs, converts to URIs, opens each as a stream, reading the contents and finally returns a map of URI->Seq[String]
 * where the key is the URI of a server, and the Strings are the contents of each of the documents retrieved from that server. 
 * Stage 2 takes the map of URI->Seq[String] resulting from stage 1 and adds the lengths of the documents (in words) to each other. The final result is a map of
 * URI->Int where the value is the total number of words read from the server represented by the key.
 * Stage 3 then sums these values together to yield a grand total. 
 * 
 * @author scalaprof
 */
object WebCrawler extends App {
  implicit val config = ConfigFactory.load
  val configApp = config.getConfig("WebCrawler")
  implicit val system = ActorSystem(configApp.getString("name"))   
  implicit val timeout: Timeout = getTimeout(config.getString("timeout"))
  import ExecutionContext.Implicits.global
  val ws = if (args.length>0) args.toSeq else Seq("http://www.htmldog.com/examples/")  
  
  def init = Seq[String]()
  val stage1: MapReduce[String,URI,Seq[String]] = MapReduceFirstFold(
      {(q, w: String) => val u = new URI(w); (getHostURI(u), Source.fromURL(u.toURL).mkString)},
      {(a: Seq[String],v: String)=>a:+v},
      init _
    )

  val stage2 = MapReducePipeFold(
      {(w: URI, gs: Seq[String])=>(w, (for(g <- gs) yield getLinks(w,g)) reduce(_++_))},
      {(a: Seq[String],v: Seq[String])=>a++v},
      init _,
      1
    )
    
  def getLinks(w: URI, g: String): Seq[String] = for (
    nsA <- HTMLParser.parse(g) \\ "a";
    nsH <- nsA \ "@href";
    nH <- nsH.apply(0)
    ) yield normalizeURL(w,nH.toString)

  val stage3 = Reduce[Seq[String],Seq[String]]({_++_})
  val crawler = stage1 compose stage2 compose stage3
  
  doCrawl(ws,Seq[String](),2).onComplete {
    case Success(n) => println(s"total links: ${n.length}")
    case Failure(x) => system.log.error(x, "Map/reduce error (typically in map function)")
  }
  
  private def doCrawl(us: Seq[String], all: Seq[String], depth: Int): Future[Seq[String]] = {
    if (depth==0) Future(all)
    Console.err.println(s"doCrawl: $us; $all; $depth")
    val (in,out) = us.partition { x => all.contains(x) }
    val wsf = crawler(out.distinct)
    for (ws <- wsf; x <- doCrawl(ws, all++us, depth-1)) yield x
  }
  
  private def getHostURI(u: java.net.URI) = new URL(u.getScheme+"://"+u.getHost).toURI
  private def normalizeURL(w: URI, w2: String) = new URL(w.toURL,w2.toString).toString
  
  def getTimeout(t: String) = {
    val durationR = """(\d+)\s*(\w+)""".r
    t match {
      case durationR(n,s) => new Timeout(FiniteDuration(n.toLong,s))
      case _ => Timeout(10 seconds)
    }
  }
}

