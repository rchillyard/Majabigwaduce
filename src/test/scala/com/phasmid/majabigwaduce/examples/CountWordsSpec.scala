/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce.examples

import java.net.URI

import org.scalamock.scalatest.MockFactory
import org.scalatest._
import org.scalatest.concurrent._
import org.scalatest.matchers.should
import org.scalatest.time._

class CountWordsSpec extends flatspec.AnyFlatSpec with should.Matchers with Futures with ScalaFutures with Inside with MockFactory {

  // NOTE: Issue #17 This test (and others) causes the following warning in the logs:
  // 2020-05-05 21:32:38,434 WARN  akka.stream.Materializer akka.stream.Log(akka://CountWords/system/Materializers/StreamSupervisor-1) - [outbound connection to [akka://ClusterSystem@127.0.0.1:2551], control stream] Upstream failed, cause: StreamTcpException: Tcp command [Connect(127.0.0.1:2551,None,List(),Some(5000 milliseconds),true)] failed because of java.net.ConnectException: Connection refused
  "CountWords" should "work for http://www.bbc.com/ http://www.cnn.com/ http://default/" in {
    val wBBC = "http://www.bbc.com/"
    val wCNN = "http://www.cnn.com/"
    val wDef = "http://default/"
    val uBBC = new URI(wBBC)
    val uCNN = new URI(wCNN)
    val uDef = new URI(wDef)
    val hc = mock[HttpClient]
    val rBBC = mock[Resource]
    (rBBC.getServer _).expects().returning(uBBC)
    (rBBC.getContent _).expects().returning(CountWordsSpec.bbcText)
    val rCNN = mock[Resource]
    (rCNN.getServer _).expects().returning(uCNN)
    (rCNN.getContent _).expects().returning(CountWordsSpec.cnnText)
    val rDef = mock[Resource]
    (rDef.getServer _).expects().returning(uDef)
    (rDef.getContent _).expects().returning(CountWordsSpec.defaultText)
    hc.getResource _ expects wBBC returning rBBC
    hc.getResource _ expects wCNN returning rCNN
    hc.getResource _ expects wDef returning rDef
    val nf = CountWords(hc, Array(wBBC, wCNN, wDef))
    whenReady(nf, timeout(Span(6, Seconds)))(i => assert(i == 556))
  }
}

//noinspection SpellCheckingInspection
object CountWordsSpec {
  // there are 556 words in total between the three extracts
  val bbcText =
    """The US military has delivered more than 45 tonnes of ammunition to rebels fighting the jihadist group Islamic State (IS) in north-eastern Syria.
C-17 transport aircraft, accompanied by fighter escorts, dropped pallets of supplies overnight in Hassakeh province, a Pentagon spokesman said.
The consignment reportedly comprised small arms, ammunition and grenades.
It comes days after the US abandoned a $500m (£326m) plan to train thousands of "moderate" rebels to fight IS.
The money will instead be used to provide much-needed ammunition and some weapons to commanders of rebel groups already established on the ground."""
  val cnnText =
    """(CNN) Vladimir Putin just confirmed what many suspected -- that Russian airstrikes in Syria are meant to bolster President Bashar al-Assad's regime.
But exactly how they're doing that remains a point of contention: Are Russians really focused on pummeling ISIS, or are they targeting Syrian rebels demanding an end to the Assad dynasty?
"Our task is to stabilize the legitimate government and to create conditions for a political compromise ... by military means, of course," Putin told the state-run Russia 24 TV.
"The units of international terrorists and their ilk have no desire to negotiate with the Syrian government, who is almost sieged in its own capital."
Russia has said it's coordinating with the Syrian regime to target ISIS and other terrorists. Al-Assad has used the term "terrorists" to describe Syrians who seek his ouster."""
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
}
