package com.socrata

import com.socrata.http.server.responses._

package object cetera {
  val HeaderAclAllowOriginAll = Header("Access-Control-Allow-Origin", "*")
  val IndexDatasets = "datasets"
  val IndexFiles = "files"
  val IndexHrefs = "hrefs"
  val IndexMaps = "maps"
  val Indices = List(IndexDatasets, IndexFiles, IndexHrefs, IndexMaps)
}
