/*
 * Copyright (c) 2018. Phasmid Software
 */

package com.phasmid.majabigwaduce.examples.countwords

import java.net.URI

class ResourceHttpClient(resourceRoot: String) extends HttpClient {
  override def apply(w: String): Resource = new Resource {
    private val uri = new URI(w)

    def getServer(): URI = new URI(uri.getScheme, uri.getHost, null)

    def getContent(): String = {
      val path = s"$resourceRoot/${uri.getHost}${uri.getPath}"
      Option(getClass.getResourceAsStream(path))
        .map(scala.io.Source.fromInputStream(_).mkString)
        .getOrElse(throw new java.io.FileNotFoundException(s"test resource not found: $path"))
    }
  }
}