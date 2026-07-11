/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce.examples.countwords

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.util.Timeout
import com.phasmid.majabigwaduce.core.*
import com.phasmidsoftware.flog.{Loggable, Loggables}
import com.typesafe.config.{Config, ConfigFactory}

import java.net.URI
import scala.concurrent.*
import scala.util.*

type Strings = Seq[String]

type ResourceFunction = String => Resource

@main def wordCounter(args: String*): Unit =
  import ExecutionContext.Implicits.global
  CountWords.doMain(args).onComplete {
    case Success(s) => println(s);
    case Failure(x) => println(s"Failure: ${x.getMessage}")
  }

/**
 * The `CountWords` class implements a word-counting pipeline using map-reduce operations.
 * It processes a sequence of strings to produce the total count of words across all inputs.
 *
 * The class leverages the following stages:
 * 1. Map stage (`stage1`): Maps input strings to tuples containing a server URI and content using the provided `resourceFunc`.
 * 2. Reduce stage (`stage2`): Transforms and reduces intermediate results to word counts.
 * 3. Final Reduction (`stage3`): Aggregates counts into a single total word count.
 *
 * The components used include:
 * - `MapReduceFirstFold.create` for building the initial mapping stage.
 * - `MapReducePipe.create` for intermediate reduction logic combining words and sums.
 * - `Reduce` for the final reduction stage.
 *
 * This class uses implicit dependencies for actor system, logging, configurations, timeouts,
 * and execution contexts to manage asynchronous processing and distributed behaviors.
 *
 * @param resourceFunc A function to fetch a `Resource` object given a string input,
 *                     representing external data accessed during the pipeline.
 *
 * @param system       An implicit `ActorSystem` used for actor-based parallelism and concurrency.
 * @param logger       An implicit `LoggingAdapter` used for logging purposes.
 * @param config       An implicit `Config` used for managing configurations.
 * @param timeout      An implicit `Timeout` for specifying operation timeout durations.
 * @param ec           An implicit `ExecutionContext` for managing asynchronous computations.
 */
case class CountWords(resourceFunc: ResourceFunction)(using system: ActorSystem, logger: LoggingAdapter, config: Config, timeout: Timeout, ec: ExecutionContext) extends (Seq[String] => Future[Int]) {

  trait StringsZeros extends Zero[Strings] {
    def zero: Strings = Nil: Strings
  }

  implicit object StringsZeros extends StringsZeros

  trait IntZeros extends Zero[Int] {
    def zero: Int = 0
  }

  implicit object IntZeros extends IntZeros

  override def apply(ws: Strings): Future[Int] =
    given actors: Actors = Actors(summon[ActorSystem], summon[Config])
    //    val stage1 = MapReduceFirstFold.create({ w: String => val u = resourceFunc("stage1 map" !! w); (u.getServer, u.getContent) }, appendString)(actors, timeout)
    val stage1 = MapReduceFirstFold.create({ (w: String) => val u = resourceFunc(w); (u.getServer(), u.getContent()) }, appendString)(actors, timeout)

    val stage2 = MapReducePipe.create[URI, Strings, URI, Int, Int](
      (w, gs) => w -> (countFields(gs) reduce addInts),
      addInts,
      1
    )
    val stage3 = Reduce[URI, Int, Int](addInts)
    val mr = stage1 & stage2 | stage3
    mr(ws)

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
object CountWords: // extends Loggables:

  def countWords(hc: HttpClient, args: Seq[String]): Future[Int] =
    given config: Config = ConfigFactory.load.getConfig("majabigwaduce.CountWords")
    given system: ActorSystem = ActorSystem(config.getString("name"))
    given ec: ExecutionContext = system.dispatcher
    given timeout: Timeout = Actors.getTimeout(config.getString("timeout"))
    given logger: LoggingAdapter = system.log
//    given iterableStringLoggable: Loggable[Iterable[String]] = iterableLoggable[String]()

    //    val flog: Flog = Flog[CountWords.type]
    //    import flog._

    val ws = if args.nonEmpty
      then args
      else Seq("https://www.bbc.com/doc1", "https://www.bbc.com/doc2", "https://www.cnn.com/doc3")
    //    "starting domains:" !! ws
    CountWords(hc.getResource).apply(ws).andThen { case _ => system.terminate() }

  /**
   * Executes the main application logic for processing a sequence of URLs to count words.
   * If no arguments are provided, default URLs will be used.
   *
   * @param args A varargs parameter representing the URLs to process.
   * @return Unit as the computation runs asynchronously and results are logged or handled.
   */
  def doMain(args: Strings): Future[String] =
    import ExecutionContext.Implicits.global
    val hc = new ResourceHttpClient("/countwords")
    CountWords.countWords(hc, args).map(x => s"Word count = $x")

/**
 * A trait that defines an HTTP client capable of resolving and handling resources.
 *
 * This trait serves as a contract for implementations that can fetch resources,
 * such as documents or content, from a given location represented by a URL or URI-like string.
 *
 * Key methods:
 * - apply(String): Resolves a given string into a `Resource` object.
 * - getResource(String): Delegates to `apply` to provide a resource for the specified URI.
 */
trait HttpClient extends (ResourceFunction):
  def apply(w: String): Resource

  def getResource(w: String): Resource = apply(w)

/**
 * A trait representing a resource that can be accessed, typically over a network or other location.
 *
 * This trait provides methods to retrieve the associated server information and the content of the resource.
 */
trait Resource:
  /**
   * Retrieves the URI of the server associated with the resource.
   *
   * This method returns the server URI where the resource is hosted, typically including the scheme and host, but excluding path or query parameters.
   *
   * NOTE that it is defined with parentheses (which generates a compiler warning).
   * It's left that way for compatibility with the mock library.
   *
   * @return the URI of the server.
   */
  def getServer(): URI

  /**
   * Retrieves the content of the resource as a string.
   *
   * This method is used to fetch the underlying data or text associated with the resource,
   * which may involve accessing network or local storage depending on the implementation.
   *
   * NOTE that it is defined with parentheses (which generates a compiler warning).
   * It's left that way for compatibility with the mock library.
   *
   * @return the content of the resource as a String
   */
  def getContent(): String
