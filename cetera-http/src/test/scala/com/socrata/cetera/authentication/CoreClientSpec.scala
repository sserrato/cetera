package com.socrata.cetera.authentication

import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.CompactJsonWriter
import org.mockserver.integration.ClientAndServer.startClientAndServer
import org.mockserver.model.HttpRequest.request
import org.mockserver.model.HttpResponse.response
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, ShouldMatchers, WordSpec}

import com.socrata.cetera.{TestHttpClient, TestCoreClient}

class CoreClientSpec extends WordSpec with ShouldMatchers with BeforeAndAfterAll with BeforeAndAfterEach {

  val coreTestPort = 8082
  val httpClient = new TestHttpClient()
  val coreClient = new TestCoreClient(httpClient, coreTestPort)
  val mockServer = startClientAndServer(coreTestPort)
  val domain = "forest.com"
  val cookie = "c=cookie"

  override def beforeEach(): Unit = {
    mockServer.reset()
  }

  override def afterAll(): Unit = {
    mockServer.stop(true)
    httpClient.close()
  }

  def setUpMockWithAuthorizedUser(): Unit = {
    val userBody =
      j"""{
        "id" : "boo-bear",
        "screenName" : "boo bear",
        "email" : "boo.bear@forest.com",
        "numberOfFollowers" : 1000,
        "numberOfFriends" : 500,
        "roleName" : "headBear",
        "rights" : [ "steal_honey", "scare_tourists"],
        "flags" : [ "admin" ]
        }"""

    mockServer.when(
      request()
        .withMethod("GET")
        .withPath("/users.json")
        .withHeader("X-Socrata-Host", domain)
    ).respond(
      response()
        .withStatusCode(200)
        .withHeader("Content-Type", "application/json; charset=utf-8")
        .withBody(CompactJsonWriter.toString(userBody))
    )
  }

  def setUpMockWithUnauthorizeddUser(): Unit = {
    val userBody = j"""{"id" : "lazy-bear"}"""

    mockServer.when(
      request()
        .withMethod("GET")
        .withPath("/users.json")
        .withHeader("X-Socrata-Host", domain)
    ).respond(
      response()
        .withStatusCode(200)
        .withHeader("Content-Type", "application/json; charset=utf-8")
        .withBody(CompactJsonWriter.toString(userBody))
    )
  }

  "The optionallyGetUserByCookie method" should {
    "return None if no cookie is given" in {
      setUpMockWithAuthorizedUser()
      val userWithoutContext = coreClient.optionallyGetUserByCookie(None, None)
      val userWithContext = coreClient.optionallyGetUserByCookie(Some(domain), None)

      userWithoutContext should be(None)
      userWithContext should be(None)
    }

    "return None if no context is given" in {
      setUpMockWithAuthorizedUser()
      val userWithoutCookie = coreClient.optionallyGetUserByCookie(None, None)
      val userWithCookie = coreClient.optionallyGetUserByCookie(None, Some(cookie))

      userWithoutCookie should be(None)
      userWithCookie should be(None)
    }

    "return Some user if both the context and cookie are given" in {
      setUpMockWithAuthorizedUser()
      val user = coreClient.optionallyGetUserByCookie(Some(domain), Some(cookie))

      user should be('defined)
      user.get should have('id ("boo-bear"))
    }
  }

  "The fetchUserByCookie method" should {
    // WARNING: Socrata-http requires that cookies have the form key=value, otherwise it will respond with
    // 'The target server failed to respond'

    "return the user if core returns a 200 and an empowered user" in {
      setUpMockWithAuthorizedUser()
      val expectedUser = User(
        "boo-bear",
        Some("headBear"),
        Some(List("steal_honey", "scare_tourists")),
        Some(List("admin")))

      val actualUser = coreClient.fetchUserByCookie(domain, cookie)
      actualUser.get should be(expectedUser)
    }

    "return the user if core returns a 200 and powerless user" in {
      setUpMockWithUnauthorizeddUser()
      val expectedUser = User("lazy-bear", None, None, None)

      val actualUser = coreClient.fetchUserByCookie(domain, cookie)
      actualUser.get should be(expectedUser)
    }


    "return None without calling core if passed an empty cookie" in {
      setUpMockWithAuthorizedUser()
      coreClient.fetchUserByCookie(domain, "") should be(None)
    }

    "return None if core returns a 401" in {
      mockServer
        .when(
          request()
            .withMethod("GET")
            .withPath("/users.json")
        )
        .respond(
          response()
            .withStatusCode(401)
        )

      coreClient.fetchUserByCookie(domain, cookie) should be(None)
    }

    "return None if core returns a 403" in {
      mockServer
        .when(
          request()
            .withMethod("GET")
            .withPath("/users.json")
        )
        .respond(
          response()
            .withStatusCode(403)
        )

      coreClient.fetchUserByCookie(domain, cookie) should be(None)
    }

    "return None if core returns a 500" in {
      mockServer
        .when(
          request()
            .withMethod("GET")
            .withPath("/users.json")
        )
        .respond(
          response()
            .withStatusCode(500)
        )

      coreClient.fetchUserByCookie(domain, cookie) should be(None)
    }
  }

  "The fetchUserById method" should {
    "return the user if core returns a 200" in {
      val fxf = "boo-bear"
      val userBody =
        j"""{
        "id" : "boo-bear",
        "screenName" : "boo bear",
        "numberOfFollowers" : 1000,
        "numberOfFriends" : 500,
        "roleName" : "headBear",
        "rights" : [ "steal_honey", "scare_tourists"],
        "flags" : [ "admin" ]
        }"""

      mockServer.when(
        request()
          .withMethod("GET")
          .withPath(s"/users/$fxf")
          .withHeader("X-Socrata-Host", domain)
      ).respond(
        response()
          .withStatusCode(200)
          .withHeader("Content-Type", "application/json; charset=utf-8")
          .withBody(CompactJsonWriter.toString(userBody))
      )

      val expectedUser = User(
        "boo-bear",
        Some("headBear"),
        Some(List("steal_honey", "scare_tourists")),
        Some(List("admin"))
      )

      val actualUser = coreClient.fetchUserById(domain, fxf)
      actualUser.get should be(expectedUser)
    }

    "return None if the core returns unexpected json" in {
      val fxf = "oops-bear"
      val userBody =
        j"""{
        "screenName" : "oops bear",
        "rights" : [ "hibernate"]
        }"""

      mockServer.when(
        request()
          .withMethod("GET")
          .withPath(s"/users/$fxf")
          .withHeader("X-Socrata-Host", domain)
      ).respond(
        response()
          .withStatusCode(200)
          .withHeader("Content-Type", "application/json; charset=utf-8")
          .withBody(CompactJsonWriter.toString(userBody))
      )
      coreClient.fetchUserById(domain, fxf) should be(None)
    }

    "return None if core returns a 405" in {
      val fxf = "four-four"
      mockServer
        .when(
          request()
            .withMethod("GET")
            .withPath(s"/users/$fxf")
        )
        .respond(
          response()
            .withStatusCode(405)
        )

      coreClient.fetchUserById(domain, fxf) should be(None)
    }
  }
}

// no mock-server set up for these tests
class CoreClientlessSpec extends WordSpec with ShouldMatchers {

  val coreTestPort = 8016
  val httpClient = new TestHttpClient()
  val coreClient = new TestCoreClient(httpClient, coreTestPort)
  val domain = "forest.com"
  val cookie = "c=cookie"

  "When core is not reachable, the fetchUserByCookie method" should {
    "return None" in {
      coreClient.fetchUserByCookie(domain, cookie) should be(None)
    }
  }

  "When core is not reachable, the fetchUserById method" should {
    "return None" in {
      coreClient.fetchUserById(domain, "four-four") should be(None)
    }
  }
}
