/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce

import akka.actor.{ActorSystem, Props}
import akka.event.LoggingAdapter
import akka.pattern._
import akka.util.Timeout
import com.phasmid.majabigwaduce.core._
import com.typesafe.config.ConfigFactory
import org.scalatest._
import org.scalatest.concurrent._
import org.scalatest.matchers.should
import org.scalatest.time._

import java.net.URL
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util._

case class MockURL(w: String) {
  def url: URL = new URL(w)

  def content: String = MapReduceFuncSpec.getMockContent(url)

  def asTuple: (URL, String) = url -> content
}

class MapReduceFuncSpec extends flatspec.AnyFlatSpec with should.Matchers with Futures with ScalaFutures with Inside {
  implicit val system: ActorSystem = ActorSystem("MapReduceFuncSpec")
  implicit val timeout: Timeout = Timeout(5 seconds)

  import system.dispatcher

  private val logger: LoggingAdapter = system.log

  private val config = ConfigFactory.load()
  val spec0 = "WC"
  val spec1 = "WC-1"
  val spec2 = "WC-2"
  val spec3 = "TF-2"

  behavior of spec1

  it should "work for http://www.bbc.com/ http://www.cnn.com/ http://default/" in {
    logger.info(s"Starting $spec1:work for http://www.bbc.com/ http://www.cnn.com/ http://default/")

    def mapper(w: String): (URL, String) = MockURL(w).asTuple

    val props = Props.create(classOf[Master_First_Fold[String, URL, String, Seq[String]]], config, MapReduce.lift(mapper _), reducer _, init _)
    //noinspection SpellCheckingInspection
    val master = system.actorOf(props, s"""mstr-$spec1""")
    val futureResponse = master.ask(Seq("http://www.bbc.com/", "http://www.cnn.com/", "http://default/"))
    val wsUrf = futureResponse.mapTo[Response[URL, Seq[String]]]
    whenReady(wsUrf, timeout(Span(6, Seconds))) {
      wsUr =>
        assert(wsUr.size == 3)
        assert(wsUr.left.isEmpty)
        assert(wsUr.right.size == 3)
    }
    system.stop(master)
  }

  behavior of spec2

  it should "yield 556" in {
    logger.info(s"Starting $spec2:yield 556")

    def mapper(w: String, gs: Seq[String]): (String, Int) = (w, (for (g <- gs) yield g.split("""\s+""").length) reduce (_ + _))

    val props = Props.create(classOf[Master[String, Seq[String], String, Int, Int]], config, MapReduce.lift(mapper _), adder _)
    val master = system.actorOf(props, s"""master-$spec2""")
    val part1result = Map[String, Seq[String]]("http://www.bbc.com/" -> Seq(MapReduceFuncSpec.bbcText),
      "http://www.cnn.com/" -> Seq(MapReduceFuncSpec.cnnText),
      "http://default/" -> Seq(MapReduceFuncSpec.defaultText))
    val iSrf = master.ask(part1result).mapTo[Response[String, Int]]
    whenReady(iSrf, timeout(Span(6, Seconds))) {
      iSr =>
        assert(iSr.size == 3)
        assert(iSr.right.get("http://www.bbc.com/") match { case Some(94) => true; case _ => false })
        assert(iSr.right.get("http://www.cnn.com/") match { case Some(135) => true; case _ => false })
        assert(iSr.right.get("http://default/") match { case Some(327) => true; case _ => false })
        assert(iSr.right.values.sum == 556)
    }
    system.stop(master)
  }

  `spec0` should "work for http://www.bbc.com/ http://www.cnn.com/ http://default/" in {
    logger.info(s"Starting $spec0:work for http://www.bbc.com/ http://www.cnn.com/ http://default/")

    def mapper1(w: String): (URL, String) = MockURL(w).asTuple

    def mapper2(w: URL, gs: Seq[String]): (URL, Int) = (w, (for (g <- gs) yield g.split("""\s+""").length) reduce (_ + _))

    val props1 = Props.create(classOf[Master_First_Fold[String, URL, String, Seq[String]]], config, MapReduce.lift(mapper1 _), reducer _, init _)
    val master1 = system.actorOf(props1, s"WC-1-master")
    val props2 = Props.create(classOf[Master[URL, Seq[String], URL, Int, Int]], config, MapReduce.lift(mapper2 _), adder _)
    val master2 = system.actorOf(props2, s"WC-2-master")
    val wsUrf = master1.ask(Seq("http://www.bbc.com/", "http://www.cnn.com/", "http://default/")).mapTo[Response[URL, Seq[String]]]
    val iUrf = wsUrf flatMap { wsUr => val wsUm = wsUr.right; master2.ask(wsUm).mapTo[Response[URL, Int]] }
    whenReady(iUrf, timeout(Span(6, Seconds))) {
      iUr =>
        assert(iUr.size == 3)
        assert(iUr.right.get(new URL("http://www.bbc.com/")) match { case Some(94) => true; case _ => false })
        assert(iUr.right.get(new URL("http://www.cnn.com/")) match { case Some(135) => true; case _ => false })
        assert(iUr.right.get(new URL("http://default/")) match { case Some(327) => true; case _ => false })
        assert(iUr.right.values.sum == 556)
    }
    system.stop(master1)
    system.stop(master2)
  }

  it should "fail because mapper is incorrectly defined" in {
    logger.info(s"Starting $spec0:fail because mapper is incorrectly defined")
    implicit val timeout: Timeout = Timeout(60 seconds) // We need a longer timeout for this one to work correctly.

    def mapper1(w: String): (URL, String) = MockURL(w).asTuple

    def mapper2(w: String, gs: Seq[String]): (String, Int) = (w, (for (g <- gs) yield g.split("""\s+""").length) reduce (_ + _))

    val props1 = Props.create(classOf[Master_First_Fold[String, URL, String, Seq[String]]], config, mapper1 _, reducer _, init _)
    val master1 = system.actorOf(props1, s"WC-1b-master")
    val props2 = Props.create(classOf[Master[URL, Seq[String], URL, Int, Int]], config, mapper2 _, adder _)
    val master2 = system.actorOf(props2, s"WC-2b-master")
    val wsUrf = master1.ask(Seq("http://www.bbc.com/", "http://www.cnn.com/", "http://default/")).mapTo[Response[URL, Seq[String]]]
    val iUrf = wsUrf flatMap { wsUr => val wsUm = wsUr.right; master2.ask(wsUm).mapTo[Response[URL, Int]] }
    iUrf.onComplete {
      case Failure(x: AskTimeoutException) => fail(s"should throw MapReduceException, not $x")
      case Failure(x) =>
        x shouldBe a[MapReduceException]
        x.getCause shouldBe a[ClassCastException]
      case Success(_) => fail("should fail")
    }
    system.stop(master1)
    system.stop(master2)
  }

  behavior of spec3

  it should "work for word map" in {
    logger.info(s"Starting $spec3:work for word map")

    def mapper(w: String, us: Seq[URL]): (String, Int) = (w, us.length)

    val props = Props.create(classOf[Master[String, Seq[URL], String, Int, Int]], config, MapReduce.lift(mapper _), adder _)
    val master = system.actorOf(props, s"TF2-master")
    val bbc = new URL("http://www.bbc.com/")
    val cnn = new URL("http://www.cnn.com/")
    val other = new URL("http://default/")
    val occurrences = Map("the" -> Seq(bbc, cnn, other), "Syria" -> Seq(bbc, cnn))
    val iSrf = master.ask(occurrences).mapTo[Response[String, Int]]
    whenReady(iSrf, timeout(Span(6, Seconds)))(iSr =>
      assert(iSr.size == 2)
    )
    system.stop(master)
  }

  private def reducer(a: Seq[String], v: String) = a :+ v

  private def init = Seq[String]()

  def adder(x: Int, y: Int): Int = x + y
}

object MapReduceFuncSpec {
  //noinspection SpellCheckingInspection,SpellCheckingInspection
  // there are 556 words in total between the three extracts
  val bbcText =
  """The US military has delivered more than 45 tonnes of ammunition to rebels fighting the jihadist group Islamic State (IS) in north-eastern Syria.
C-17 transport aircraft, accompanied by fighter escorts, dropped pallets of supplies overnight in Hassakeh province, a Pentagon spokesman said.
The consignment reportedly comprised small arms, ammunition and grenades.
It comes days after the US abandoned a $500m (£326m) plan to train thousands of "moderate" rebels to fight IS.
The money will instead be used to provide much-needed ammunition and some weapons to commanders of rebel groups already established on the ground."""
  //noinspection SpellCheckingInspection,SpellCheckingInspection,SpellCheckingInspection
  val cnnText =
    """(CNN) Vladimir Putin just confirmed what many suspected -- that Russian airstrikes in Syria are meant to bolster President Bashar al-Assad's regime.
But exactly how they're doing that remains a point of contention: Are Russians really focused on pummeling ISIS, or are they targeting Syrian rebels demanding an end to the Assad dynasty?
"Our task is to stabilize the legitimate government and to create conditions for a political compromise ... by military means, of course," Putin told the state-run Russia 24 TV.
"The units of international terrorists and their ilk have no desire to negotiate with the Syrian government, who is almost sieged in its own capital."
Russia has said it's coordinating with the Syrian regime to target ISIS and other terrorists. Al-Assad has used the term "terrorists" to describe Syrians who seek his ouster."""
  //noinspection SpellCheckingInspection,SpellCheckingInspection,SpellCheckingInspection,SpellCheckingInspection,SpellCheckingInspection,SpellCheckingInspection
  val defaultText =
    """U.S. forces airdropped small arms ammunition and other supplies to Syrian Arab rebels, barely two weeks after Russia raised the stakes in the long-running civil war by intervening on the side of President Bashar al-Assad.
One military official said the drop, by Air Force C-17 cargo planes in northern Syria on Sunday, was part of a revamped U.S. strategy announced last week to help rebels in Syria battling Islamic State militants.
Last week, Washington shelved a program to train and equip "moderate" rebels opposed to Assad who would join the fight against Islamic State.[:nL1N1221MR]
The only group on the ground to have success against Islamic State while cooperating with the U.S.-led coalition is a Kurdish militia, the YPG, which has carved out an autonomous zone in northern Syria and advanced deep into Islamic State's stronghold Raqqa province.
On Monday, the YPG announced a new alliance with small groups of Arab fighters, which could help deflect criticism that it fights only on behalf of Kurds. Washington has indicated it could direct funding and weapons to Arab commanders on the ground who cooperate with the YPG.
Syrian Arab rebels said they had been told by Washington that new weapons were on their way to help them launch a joint offensive with their Kurdish allies on the city of Raqqa, the de facto Islamic State capital.
The U.S. military confirmed dropping supplies to opposition fighters vetted by the United States but would say no more about the groups that received the supplies or the type of equipment in the airdrop.[:nL1N12C115]
The Russian intervention in the four-year Syrian war has caught U.S. President Barack Obama's administration off guard. Washington has been trying to defeat Islamic State while still calling for Assad's downfall.
DANGEROUS CONSEQUENCES
Russian President Vladimir Putin was rebuffed in his bid to gain support for his country's bombing campaign, with Saudi sources saying they had warned the Kremlin leader of dangerous consequences and Europe issuing its strongest criticism yet."""

  def getMockContent(u: URL): String = {
    u.getHost match {
      case "www.bbc.com" => bbcText
      case "www.cnn.com" => cnnText
      case _ => defaultText
    }
  }
}
