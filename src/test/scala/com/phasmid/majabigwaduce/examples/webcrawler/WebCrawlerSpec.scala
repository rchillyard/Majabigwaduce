/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce.examples.webcrawler

import akka.actor.ActorSystem
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.{Futures, ScalaFutures}
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{FlatSpec, Inside, Matchers}

import scala.concurrent.ExecutionContext
import scala.language.postfixOps

/**
  * NOTE: this is more of a Functional test rather than a unit test.
  * Feel free to ignore this test if it's taking up too much time.
  *
  * Created by scalaprof on 6/28/16.
  */
class WebCrawlerSpec extends FlatSpec with Matchers with Futures with ScalaFutures with Inside with MockFactory {
  // CONSIDER when run alone, this works just fine.
  // CONSIDER moving to it since this requires an internet connection
  // But sometimes when run with all the specs in Majabigwaduce, this runs -- but in the logs we see exceptions thrown
  "crawl" should "work" in {
    implicit val config: Config = ConfigFactory.load.getConfig("WebCrawler")
    implicit val system: ActorSystem = ActorSystem(config.getString("name"))
    implicit val to: Timeout = WebCrawler.getTimeout(config.getString("timeout"))
    import ExecutionContext.Implicits.global
    val ws = Seq(config.getString("start"))
    val crawler = WebCrawler(config.getInt("depth"))
    val xf = crawler(ws)
    whenReady(xf, timeout(Span(300, Seconds)))(// The actual number is approximate and will vary (currently 9)
      i => assert(i > 5 && i < 200))
  }
}
