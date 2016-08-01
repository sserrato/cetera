package com.socrata.cetera.auth

import org.scalatest.{WordSpec, ShouldMatchers}

import com.socrata.http.server.HttpRequest

import com.socrata.cetera.{HeaderAuthorizationKey, HeaderCookieKey}

class AuthParamsSpec extends WordSpec with ShouldMatchers {
  "The areDefined method" should {
    "be true if either a cookie or a basic auth parameter is provided, and false otherwise" in {
      AuthParams(cookie=Some("cookie")).areDefined should be(true)
      AuthParams(basicAuth=Some("123456789")).areDefined should be(true)
      AuthParams().areDefined should be(false)
    }
  }

  "The headers method" should {
    "return an empty list given no auth params" in {
      AuthParams().headers should be(Nil)
    }

    "return a cookie header given just a cookie" in {
      val cookie = "_type=chocolatechip"
      AuthParams(cookie=Some(cookie)).headers should be(List((HeaderCookieKey, cookie)))
    }

    "return just an auth string given a basic auth header" in {
      val basicAuth = "Basic cHJvZmVzc29yeDpjZXJlYnJvNGxpZmU="
      AuthParams(basicAuth=Some(basicAuth)).headers should be(List((HeaderAuthorizationKey, basicAuth)))
    }

    "return both a cookie and an auth header when both are provided" in {
      val cookie = "_type=chocolatechip"
      val basicAuth = "Basic cHJvZmVzc29yeDpjZXJlYnJvNGxpZmU="
      val authParams = AuthParams(basicAuth=Some(basicAuth), cookie=Some(cookie))
      val expectedHeaders = List((HeaderAuthorizationKey, basicAuth), (HeaderCookieKey, cookie))
      authParams.headers should be(expectedHeaders)
    }
  }
}
