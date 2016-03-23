package com.phasmid.majabigwaduce.examples

import scala.util._
import scala.concurrent._
import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory
import akka.actor.{ActorSystem, Props}
import akka.util.Timeout
import akka.util.Timeout._
import akka.pattern.ask
import java.net.URI
import com.phasmid.majabigwaduce._

trait HttpClient {
  def getResource(w: String): Resource
}
trait Resource {
  def getServer: URI
  def getContent: String
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
  def apply(hc: HttpClient, args: Array[String]) = {
  val configRoot = ConfigFactory.load
  implicit val config = configRoot.getConfig("CountWords")
  implicit val system = ActorSystem(config.getString("name"))   
  implicit val timeout: Timeout = getTimeout(config.getString("timeout"))
  import ExecutionContext.Implicits.global
  
  def init = Seq[String]()
  val stage1 = MapReduceFirstFold(
      {w: String => val u = hc.getResource(w); system.log.debug(s"stage1 map: $w"); (u.getServer, u.getContent)},
      {(a: Seq[String],v: String)=>a:+v},
      init _
    )
  val stage2 = MapReducePipe(
      {(w: URI, gs: Seq[String])=>(w, (for(g <- gs) yield g.split("""\s+""").length) reduce(_+_))},
      {(x: Int, y: Int)=>x+y},
      1
    )
  val stage3 = Reduce[Int,Int]({_+_})
  val countWords = stage1 compose stage2 compose stage3
  
  val ws = if (args.length>0) args.toSeq else Seq("http://www.bbc.com/doc1", "http://www.cnn.com/doc2", "http://default/doc3", "http://www.bbc.com/doc2", "http://www.bbc.com/doc3")  
  
  countWords.apply(ws)
}
  
  // TODO try to combine this with the same method in MapReduceActor
  def getTimeout(t: String) = {
    val durationR = """(\d+)\s*(\w+)""".r
    t match {
      case durationR(n,s) => new Timeout(FiniteDuration(n.toLong,s))
      case _ => Timeout(10 seconds)
    }
  }
}
