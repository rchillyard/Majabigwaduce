/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce.examples.webcrawler

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.{Futures, ScalaFutures}
import org.scalatest.matchers.should
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{Inside, flatspec}

import java.net.{HttpURLConnection, URL}
import scala.concurrent.ExecutionContext
import scala.util.Try

/**
 * NOTE: this is more of a Functional test rather than a unit test.
 * Feel free to ignore this test if it's taking up too much time.
 *
 * Created by scalaprof on 6/28/16.
 */
class WebCrawlerSpec extends flatspec.AnyFlatSpec with should.Matchers with Futures with ScalaFutures with Inside with MockFactory {
  // CONSIDER when run alone, this works just fine.
  // CONSIDER moving to it since this requires an internet connection
  // But sometimes when run with all the specs in Majabigwaduce, this runs -- but in the logs we see exceptions thrown
  "crawl" should "work" in {
    given config: Config = ConfigFactory.load.getConfig("majabigwaduce.WebCrawler")

    given system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, config.getString("name"))

    given to: Timeout = WebCrawler.getTimeout(config.getString("timeout"))
    import ExecutionContext.Implicits.global
    val ws = Seq(config.getString("start"))

    // NOTE: WebCrawler's own reduce function is deliberately forgiving of a failed fetch (it
    // logs and moves on, rather than crashing a whole crawl over one bad link) -- appropriate for
    // production use, but it means a network problem reaching the seed page doesn't fail this
    // Future, it just silently yields an implausibly small link count. So we check reachability
    // of the seed page directly, up front: if it's unreachable, this is an environment problem,
    // not a code problem, and we cancel (not fail) the test; if it's reachable, a low link count
    // is a real regression and the strict assertion below should hold.
    if !WebCrawlerSpec.isReachable(ws.head)
    then cancel(s"seed URL ${ws.head} is not reachable from this environment -- skipping live-network crawl test")
    else
      val crawler = WebCrawler(config.getInt("depth"))
      val xf = crawler(ws)
      whenReady(xf, timeout(Span(300, Seconds)))( // The actual number is approximate and will vary (currently 9)
        i => assert(i > 5 && i < 200))
  }

  "webCrawlerApp main program" should "work" in {
    webCrawlerApp()
  }
}

object WebCrawlerSpec:
  private def isReachable(url: String): Boolean =
    Try {
      val connection = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
      connection.setConnectTimeout(5000)
      connection.setReadTimeout(5000)
      connection.setRequestMethod("GET")
      val code = connection.getResponseCode
      connection.disconnect()
      code >= 200 && code < 400
    }.getOrElse(false)
