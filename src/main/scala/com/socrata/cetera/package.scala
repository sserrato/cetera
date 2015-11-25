package com.socrata

import com.socrata.http.server.responses._

package object cetera {
  val HeaderAclAllowOriginAll = Header("Access-Control-Allow-Origin", "*")

  val IndexCatalog = "catalog"
  val Indices = List(IndexCatalog)

  val esDocumentType = "document"
  val esDomainType = "domain"
}
