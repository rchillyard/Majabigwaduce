/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce.examples
package webcrawler

import java.io.ByteArrayInputStream

import org.ccil.cowan.tagsoup.jaxp.SAXFactoryImpl
import org.xml.sax.InputSource

import scala.xml.Node
import scala.xml.parsing.NoBindingFactoryAdapter

/**
  * @author scalaprof
  */
object HTMLParser {

  lazy val adapter = new NoBindingFactoryAdapter()
  private lazy val parser = (new SAXFactoryImpl).newSAXParser

  def parse(html: String, encoding: String = "UTF-8"): Node = {
    this.parse(html.getBytes(encoding))
  }

  def parse(html: Array[Byte]): Node = {
    val stream = new ByteArrayInputStream(html)
    val source = new InputSource(stream)
    adapter.loadXML(source, parser)
  }
}