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

  override def beforeEach(): Unit = {
    mockServer.reset()
  }

  override def afterAll(): Unit = {
    mockServer.stop(true)
    httpClient.close()
  }

  "The fetchCurrentUser method" should {
    // WARNING: Socrata-http requires that cookies have the form key=value, otherwise it will respond with
    // 'The target server failed to respond'

    "return the user if core returns a 200 and an empowered user" in {

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

      val domain = "forest.com"
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

      val expectedUser = User("boo-bear", "boo.bear@forest.com", Some("headBear"),
        Some(List("steal_honey", "scare_tourists")), Some(List("admin")))

      val actualUser = coreClient.fetchCurrentUser(domain, Some("c=cookie"))
      actualUser.get should be(expectedUser)
    }

    "return the user if core returns a 200 and powerless user" in {

      val userBody =
        j"""{
        "id" : "lazy-bear",
        "screenName" : "lazy bear",
        "email" : "lazy.bear@forest.com"
        }"""

      val domain = "forest.com"
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

      val expectedUser = User("lazy-bear", "lazy.bear@forest.com", None, None, None)

      val actualUser = coreClient.fetchCurrentUser(domain, Some("c=cookie"))
      actualUser.get should be(expectedUser)
    }


    "return None without calling core if passed an empty cookie" in {
      coreClient.fetchCurrentUser("forest.com", Some("")) should be(None)
    }

    "return None without calling core if passed no cookie" in {
      coreClient.fetchCurrentUser("forest.com", None) should be(None)
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

      coreClient.fetchCurrentUser("forest.com", Some("c=cookie")) should be(None)
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

      coreClient.fetchCurrentUser("forest.com", Some("c=cookie")) should be(None)
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

      coreClient.fetchCurrentUser("forest.com", Some("c=cookie")) should be(None)
    }
  }
}

// no mock-server set up for these tests
class CoreClientlessSpec extends WordSpec with ShouldMatchers {

  val coreTestPort = 8016
  val httpClient = new TestHttpClient()
  val coreClient = new TestCoreClient(httpClient, coreTestPort)

  "When core is not reachable, the fetchCurrentUser method" should {
    "return None" in {
      coreClient.fetchCurrentUser("forest.com", Some("c=cookie")) should be(None)
    }
  }
}
