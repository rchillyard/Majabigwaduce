/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce.examples.countwords

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.util.Timeout
import com.phasmid.majabigwaduce.core._
import com.phasmidsoftware.flog.{Loggable, Loggables}
import com.typesafe.config.{Config, ConfigFactory}

import java.net.URI
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
  type Strings = Seq[String]

  trait StringsZeros extends Zero[Strings] {
    def zero: Strings = Nil: Strings
  }

  implicit object StringsZeros extends StringsZeros

  trait IntZeros extends Zero[Int] {
    def zero: Int = 0
  }

  implicit object IntZeros extends IntZeros

  override def apply(ws: Strings): Future[Int] = {

    implicit val actors: Actors = Actors(implicitly[ActorSystem], implicitly[Config])

    //    val flog = Flog[CountWords]
    //    import flog._

    //    val stage1 = MapReduceFirstFold.create({ w: String => val u = resourceFunc("stage1 map" !! w); (u.getServer, u.getContent) }, appendString)(actors, timeout)
    val stage1 = MapReduceFirstFold.create({ w: String => val u = resourceFunc(w); (u.getServer, u.getContent) }, appendString)(actors, timeout)

    val stage2 = MapReducePipe.create[URI, Strings, URI, Int, Int](
      (w, gs) => w -> (countFields(gs) reduce addInts),
      addInts,
      1
    )
    val stage3 = Reduce[URI, Int, Int](addInts)
    val mr = stage1 & stage2 | stage3
    mr(ws)
  }

  private def countFields(gs: Strings) = for (g <- gs) yield g.split("""\s+""").length

  private def addInts(x: Int, y: Int) = x + y

  private def appendString(a: Strings, v: String) = a :+ v
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
object CountWords extends App with Loggables {


  def apply(hc: HttpClient, args: Array[String]): Future[Int] = {
    implicit val config: Config = ConfigFactory.load.getConfig("CountWords")
    implicit val system: ActorSystem = ActorSystem(config.getString("name"))
    implicit val timeout: Timeout = getTimeout(config.getString("timeout"))
    implicit val logger: LoggingAdapter = system.log
    import ExecutionContext.Implicits.global
    //    import Init._

    //    val flog: Flog = Flog[CountWords.type]
    //    import flog._

    implicit val iterableLoggable: Loggable[Iterable[String]] = new Loggables {}.iterableLoggable[String]()

    val ws = if (args.length > 0) args.toSeq else Seq("https://www.bbc.com/doc1", "https://www.cnn.com/doc2", "https://default/doc3", "https://www.bbc.com/doc2", "https://www.bbc.com/doc3")
    //    "starting domains:" !! ws
    CountWords(hc.getResource).apply(ws)
  }

  // TODO try to combine this with the same method in MapReduceActor
  // TODO make this more visible (it is used in matrix package also)
  def getTimeout(t: String): Timeout = {
    val durationR = """(\d+)\s*(\w+)""".r
    t match {
      case durationR(n, s) => new Timeout(FiniteDuration(n.toLong, s))
      case _ => Timeout(10 seconds)
    }
  }
}