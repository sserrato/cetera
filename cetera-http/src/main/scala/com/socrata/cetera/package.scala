package com.socrata

import com.socrata.http.server.responses._

package object cetera {
  val HeaderAclAllowOriginAll = Header("Access-Control-Allow-Origin", "*")
  val HeaderCookieKey = "Cookie"
  val HeaderSetCookieKey = "Set-Cookie"
  val HeaderXSocrataHostKey = "X-Socrata-Host"
  val HeaderXSocrataRequestIdKey = "X-Socrata-RequestId"

  val esDocumentType = "document"
  val esDomainType = "domain"
  val esUserType = "user"
}
