package com.socrata.cetera.util

import com.socrata.http.server.HttpResponse
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import org.slf4j.LoggerFactory

import com.socrata.cetera._

object Http {
  lazy val logger = LoggerFactory.getLogger(Http.getClass)

  def decorate(content: HttpResponse, status: StatusResponse, setCookies: Seq[String]): HttpResponse = {
    val baseResponse = status ~> HeaderAclAllowOriginAll
    val responseWithCookies = setCookies.foldLeft(baseResponse){ case (r, s) => r ~> Header(HeaderSetCookieKey, s) }
    logger.debug(LogHelper.formatHttpResponseHeaders(setCookies))
    responseWithCookies ~> content
  }
}
