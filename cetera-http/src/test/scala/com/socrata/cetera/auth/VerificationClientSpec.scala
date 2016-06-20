package com.socrata.cetera.auth

import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.CompactJsonWriter
import org.mockserver.integration.ClientAndServer._
import org.mockserver.model.HttpRequest._
import org.mockserver.model.HttpResponse._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, ShouldMatchers, WordSpec}

import com.socrata.cetera.{HeaderXSocrataHostKey, TestCoreClient, TestHttpClient}

class VerificationClientSpec extends WordSpec with ShouldMatchers with BeforeAndAfterAll with BeforeAndAfterEach {

  val coreTestPort = 8083
  val httpClient = new TestHttpClient()
  val coreClient = new TestCoreClient(httpClient, coreTestPort)
  val mockServer = startClientAndServer(coreTestPort)
  val verificationClient = new VerificationClient(coreClient)
  val domain = "forest.com"
  val cookie = "c=cookie"

  def calculateAuthSet(cookie: Option[String]): (Boolean, Boolean, Boolean) = {
    val (authedAsAdmin, _) = verificationClient.fetchUserAuthorization(Some(domain), cookie,
      None, { u: User => u.isAdmin })

    val (authedToSeeUsers, _) = verificationClient.fetchUserAuthorization(Some(domain), cookie,
      None, { u: User => u.canViewUsers })

    val (authedToSeeCatalog, _) = verificationClient.fetchUserAuthorization(Some(domain), cookie,
      None, { u: User => u.canViewCatalog })

    (authedAsAdmin, authedToSeeUsers, authedToSeeCatalog)
  }

  "A super admin user" should {
    "be authorized to see users and the catalog" in {
      val userBody =
        j"""{
        "id" : "dispatcher",
        "screenName" : "Mookie",
        "email" : "mookie@pandemic.com",
        "rights" : [ "move_people"],
        "flags" : [ "admin" ]
        }"""

      mockServer.when(
        request()
          .withMethod("GET")
          .withPath("/users.json")
          .withHeader(HeaderXSocrataHostKey, domain)
      ).respond(
        response()
          .withStatusCode(200)
          .withHeader("Content-Type", "application/json; charset=utf-8")
          .withBody(CompactJsonWriter.toString(userBody))
      )
      val (authedAsAdmin, authedToSeeUsers, authedToSeeCatalog) = calculateAuthSet(Some(cookie))

      authedAsAdmin should be(true)
      authedToSeeUsers should be(true)
      authedToSeeCatalog should be(true)
    }
  }

  "An admin user" should {
    "be authorized to see users and the catalog" in {
      val userBody =
        j"""{
        "id" : "quarantine-specialist",
        "screenName" : "Suzzette",
        "email" : "suzzette@pandemic.com",
        "roleName" : "administrator",
        "rights" : [ "quarantine_cities"]
        }"""

      mockServer.when(
        request()
          .withMethod("GET")
          .withPath("/users.json")
          .withHeader(HeaderXSocrataHostKey, domain)
      ).respond(
        response()
          .withStatusCode(200)
          .withHeader("Content-Type", "application/json; charset=utf-8")
          .withBody(CompactJsonWriter.toString(userBody))
      )
      val (authedAsAdmin, authedToSeeUsers, authedToSeeCatalog) = calculateAuthSet(Some(cookie))

      authedAsAdmin should be(true)
      authedToSeeUsers should be(true)
      authedToSeeCatalog should be(true)
    }
  }

  "A non-admin roled user" should {
    "be authorized to see the catalog, but not users" in {
      val userBody =
        j"""{
        "id" : "anna-belle",
        "screenName" : "researcher",
        "email" : "anna.belle@pandemic.com",
        "rights" : [ "share_info"],
        "roleName" : "researcher"
        }"""

      mockServer.when(
        request()
          .withMethod("GET")
          .withPath("/users.json")
          .withHeader(HeaderXSocrataHostKey, domain)
      ).respond(
        response()
          .withStatusCode(200)
          .withHeader("Content-Type", "application/json; charset=utf-8")
          .withBody(CompactJsonWriter.toString(userBody))
      )
      val (authedAsAdmin, authedToSeeUsers, authedToSeeCatalog) = calculateAuthSet(Some(cookie))

      authedAsAdmin should be(false)
      authedToSeeUsers should be(false)
      authedToSeeCatalog should be(true)
    }
  }

  "A non-admin, non-roled user" should {
    "be not be authorized to see the catalog or the users" in {
      val userBody =
        j"""{
        "id" : "medic",
        "screenName" : "Sam-I-am",
        "email" : "sam.i.am@pandemic.com",
        "rights" : [ "cure_disease"]
        }"""

      mockServer.when(
        request()
          .withMethod("GET")
          .withPath("/users.json")
          .withHeader(HeaderXSocrataHostKey, domain)
      ).respond(
        response()
          .withStatusCode(200)
          .withHeader("Content-Type", "application/json; charset=utf-8")
          .withBody(CompactJsonWriter.toString(userBody))
      )
      val (authedAsAdmin, authedToSeeUsers, authedToSeeCatalog) = calculateAuthSet(Some(cookie))

      authedAsAdmin should be(false)
      authedToSeeUsers should be(false)
      authedToSeeCatalog should be(false)
    }
  }

  "An anonymous user" should {
    "be not be authorized to see the catalog or the users" in {
      val userBody =
        j"""{
        "id" : "civilian"
        }"""

      mockServer.when(
        request()
          .withMethod("GET")
          .withPath("/users.json")
          .withHeader(HeaderXSocrataHostKey, domain)
      ).respond(
        response()
          .withStatusCode(200)
          .withHeader("Content-Type", "application/json; charset=utf-8")
          .withBody(CompactJsonWriter.toString(userBody))
      )
      val (authedAsAdmin, authedToSeeUsers, authedToSeeCatalog) = calculateAuthSet(None)

      authedAsAdmin should be(false)
      authedToSeeUsers should be(false)
      authedToSeeCatalog should be(false)
    }
  }
}
