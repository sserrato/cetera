package com.socrata.cetera.auth

import com.socrata.http.server.HttpRequest

import com.socrata.cetera.{HeaderAuthorizationKey, HeaderCookieKey}

case class AuthParams(
    basicAuth: Option[String] = None,
    oAuth: Option[String] = None,
    cookie: Option[String] = None) {

  def areDefined: Boolean = basicAuth.isDefined || oAuth.isDefined || cookie.isDefined

  def headers: List[(String, String)] =
    List(
      basicAuth.map((HeaderAuthorizationKey, _)),
      oAuth.map((HeaderAuthorizationKey, _)),
      cookie.map((HeaderCookieKey, _))
    ).flatten
}

object AuthParams {
  def fromHttpRequest(req: HttpRequest): AuthParams =
    AuthParams(
      req.header(HeaderAuthorizationKey).filter(_.trim.startsWith("Basic")),
      req.header(HeaderAuthorizationKey).filter(_.trim.startsWith("OAuth")),
      req.header(HeaderCookieKey))
}
