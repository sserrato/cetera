package com.socrata.cetera.search

import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.CompactJsonWriter
import org.mockserver.integration.ClientAndServer.startClientAndServer
import org.mockserver.matchers.Times
import org.mockserver.model.HttpRequest.request
import org.mockserver.model.HttpResponse.response
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, ShouldMatchers, WordSpec}

import com.socrata.cetera.types.Domain
import com.socrata.cetera.{TestHttpClient, TestCoreClient, TestESClient, TestESData}

// Please see https://github.com/socrata/cetera/blob/master/cetera-http/src/test/resources/domains.tsv
// if you have any questions about which domains are being used in these tests
class DomainClientSpec extends WordSpec with ShouldMatchers with TestESData
  with BeforeAndAfterAll with BeforeAndAfterEach {

  val coreTestPort = 8030
  val client = new TestESClient(testSuiteName)
  val httpClient = new TestHttpClient()
  val coreClient = new TestCoreClient(httpClient, coreTestPort)
  val domainClient = new DomainClient(client, coreClient, testSuiteName)

  val mockServer = startClientAndServer(coreTestPort)
  val unlockedDomain0 = domains(0)
  val unlockedDomain1 = domains(1)
  val unlockedDomain2 = domains(2)
  val lockedDomain = domains(6)
  val apiLockedDomain = domains(7)
  val doublyLockedDomain = domains(8)

  override def beforeEach(): Unit = {
    mockServer.reset()
  }

  override protected def beforeAll(): Unit = {
    bootstrapData()
  }

  override protected def afterAll(): Unit = {
    removeBootstrapData()
    client.close()
    mockServer.stop(true)
    httpClient.close()
  }

  "find" should {
    "return the domain if it exists : petercetera.net" in {
      val expectedDomain = domains(0)
      val (actualDomain, _) = domainClient.find("petercetera.net")
      actualDomain.get should be(expectedDomain)
    }

    "return the domain if it exists : opendata-demo.socrata.com" in {
      val expectedDomain = domains(1)
      val (actualDomain, _) = domainClient.find("opendata-demo.socrata.com")
      actualDomain.get should be(expectedDomain)
    }

    "return None if the domain does not exist" in {
      val expectedDomain = None
      val (actualDomain, _) = domainClient.find("hellcat.com")
      actualDomain should be(expectedDomain)
    }

    "return None if searching for blank string" in {
      val expectedDomain = None
      val (actualDomain, _) = domainClient.find("")
      actualDomain should be(expectedDomain)
    }

    "return only domains with an exact match" in {
      val expectedDomain = domains(4)
      val (actualDomain, _) = domainClient.find("dylan.demo.socrata.com")
      actualDomain shouldBe Some(expectedDomain)
    }
  }

  "findRelevantDomains" should {
    "return the context if it exists among customer domains: petercetera.net" in {
      val expectedContext = domains(0)
      val (actualContext, _, _) = domainClient.findRelevantDomains(Some("petercetera.net"), None, None)
      actualContext.get should be(expectedContext)
    }

    "return the domain if it exists among the given cnames : opendata-demo.socrata.com" in {
      val expectedContext = domains(1)
      val (actualContext, _, _) = domainClient.findRelevantDomains(
        Some("opendata-demo.socrata.com"), Some(Set("opendata-demo.socrata.com")), None)
      actualContext.get should be(expectedContext)
    }

    "return all the unlocked customer domains if not given cnames" in {
      val unlockedDomains = Set(domains(0), domains(2), domains(3), domains(4))
      val (_, actualDomains, _) = domainClient.findRelevantDomains(None, None, None)
      actualDomains should be(unlockedDomains)
    }

    "return all the unlocked domains among the given cnames if they exist" in {
      val expectedDomains = Set(domains(3), domains(4))
      val wantedDomains = Some(expectedDomains.map(d => d.domainCname))
      val (_, actualDomains, _) = domainClient.findRelevantDomains(None, wantedDomains, None)
      actualDomains should be(expectedDomains)
    }

    "return all the requested domains (locked or unlocked) if a good cookie is passed in" in {
      val context = domains(8)
      val wantedDomains = Set(domains(0), domains(6), domains(8))
      val wantedCnames = wantedDomains.map(_.domainCname)

      val userBody =
        j"""{
        "id" : "boo-bear",
        "roleName" : "headBear"
        }"""

      mockServer.when(
        request()
          .withMethod("GET")
          .withPath("/users.json")
          .withHeader("X-Socrata-Host", context.domainCname)
      ).respond(
        response()
          .withStatusCode(200)
          .withHeader("Content-Type", "application/json; charset=utf-8")
          .withBody(CompactJsonWriter.toString(userBody))
      )
      mockServer.when(
        request()
          .withMethod("GET")
          .withPath("/users/boo-bear")
      ).respond(
        response()
          .withStatusCode(200)
          .withHeader("Content-Type", "application/json; charset=utf-8")
          .withBody(CompactJsonWriter.toString(userBody))
      )

      val (actualContext, actualDomains, _) = domainClient.findRelevantDomains(Some(context.domainCname),
        Some(wantedCnames), Some("c=cookie"))
      actualContext.get should be(context)
      actualDomains should be(wantedDomains)
    }

    "throw DomainNotFound exception when searchContext is missing" in {
      intercept[DomainNotFound] {
        domainClient.findRelevantDomains(Some("iamnotarealdomain.wat"), None, None)
      }
    }

    "throw DomainNotFound exception when searchContext is missing even if domains are found" in {
      intercept[DomainNotFound] {
        domainClient.findRelevantDomains(Some("iamnotarealdomain.wat"), Some(Set("dylan.demo.socrata.com")), None)
      }
    }

    "not throw DomainNotFound exception when searchContext is present" in {
      noException should be thrownBy {
        domainClient.findRelevantDomains(Some("dylan.demo.socrata.com"), None, None)
      }
    }

    "not throw DomainNotFound exception when domains are missing" in {
      noException should be thrownBy {
        domainClient.findRelevantDomains(None, Some(Set("iamnotarealdomain.wat")), None)
      }
    }
  }

  "removeLockedDomainsFromUnauthorizedUsers" should {
    "return None for a locked context if the user has no cookie" in {
      val withoutDomains = domainClient.removeLockedDomainsForbiddenToUser(
        Some(lockedDomain), Set.empty[Domain], None)
      withoutDomains should be((None, Set.empty[Domain]))

      val withDomains = domainClient.removeLockedDomainsForbiddenToUser(
        Some(lockedDomain), Set(domains(1)), None)
      withDomains should be((None, Set(domains(1))))
    }

    "return None for a locked context if the user has a bad cookie" in {
      mockServer.when(
        request()
          .withMethod("GET")
          .withPath("/users.json")
          .withHeader("X-Socrata-Host", apiLockedDomain.domainCname),
        Times.exactly(2)
      ).respond(
        response()
          .withStatusCode(401)
      )

      val withoutDomains = domainClient.removeLockedDomainsForbiddenToUser(
        Some(apiLockedDomain), Set.empty[Domain], Some("c=cookie"))
      withoutDomains should be((None, Set.empty[Domain]))

      val withDomains = domainClient.removeLockedDomainsForbiddenToUser(
        Some(apiLockedDomain), Set(domains(1)), Some("c=cookie"))
      withDomains should be((None, Set(domains(1))))
    }

    "return None for a locked context if the user has no role" in {
      val userBody =
        j"""{
        "id" : "lazy-bear",
        "screenName" : "lazy bear",
        "email" : "lazy.bear@forest.com"
        }"""

      mockServer.when(
        request()
          .withMethod("GET")
          .withPath("/users.json")
          .withHeader("X-Socrata-Host", doublyLockedDomain.domainCname),
        Times.exactly(2)
      ).respond(
        response()
          .withStatusCode(200)
          .withHeader("Content-Type", "application/json; charset=utf-8")
          .withBody(CompactJsonWriter.toString(userBody))
      )

      val withoutDomains = domainClient.removeLockedDomainsForbiddenToUser(
        Some(doublyLockedDomain), Set.empty[Domain], Some("c=cookie"))
      withoutDomains should be((None, Set.empty[Domain]))

      val withDomains = domainClient.removeLockedDomainsForbiddenToUser(
        Some(doublyLockedDomain), Set(domains(1)), Some("c=cookie"))
      withDomains should be((None, Set(domains(1))))
    }

    "return the locked down context if the user is logged in and has a role" in {
      val userBody =
        j"""{
        "id" : "boo-bear",
        "roleName" : "headBear",
        "rights" : [ "steal_honey", "scare_tourists"],
        "flags" : [ "admin" ]
        }"""

      mockServer.when(
        request()
          .withMethod("GET")
          .withPath("/users.json")
          .withHeader("X-Socrata-Host", lockedDomain.domainCname)
      ).respond(
        response()
          .withStatusCode(200)
          .withHeader("Content-Type", "application/json; charset=utf-8")
          .withBody(CompactJsonWriter.toString(userBody))
      )
      mockServer.when(
        request()
          .withMethod("GET")
          .withPath("/users/boo-bear")
          .withHeader("X-Socrata-Host", lockedDomain.domainCname)
      ).respond(
        response()
          .withStatusCode(200)
          .withHeader("Content-Type", "application/json; charset=utf-8")
          .withBody(CompactJsonWriter.toString(userBody))
      )

      val res = domainClient.removeLockedDomainsForbiddenToUser(
        Some(lockedDomain), Set(lockedDomain), Some("c=cookie"))
      res should be((Some(lockedDomain), Set(lockedDomain)))
    }

    "remove locked domains if the user has no cookie" in {
      val relevantDomains = Set(unlockedDomain0, lockedDomain)
      val withoutContext = domainClient.removeLockedDomainsForbiddenToUser(None, relevantDomains, None)
      withoutContext should be((None, Set(unlockedDomain0)))

      val withContext = domainClient.removeLockedDomainsForbiddenToUser(
        Some(unlockedDomain0), relevantDomains, None)
      withContext should be((Some(unlockedDomain0), Set(unlockedDomain0)))
    }

    "remove locked domains if the user has a bad cookie" in {
      val relevantDomains = Set(unlockedDomain0, doublyLockedDomain)

      mockServer.when(
        request()
          .withMethod("GET")
          .withPath("/users.json")
          .withHeader("X-Socrata-Host", unlockedDomain0.domainCname)
      ).respond(
        response()
          .withStatusCode(401)
      )

      val res = domainClient.removeLockedDomainsForbiddenToUser(Some(unlockedDomain0), relevantDomains, None)
      res should be((Some(unlockedDomain0), Set(unlockedDomain0)))
    }

    "remove locked domains if the user has a good cookie, but no role on the locked domain (where domain is the context)" in {
      val relevantDomains = Set(unlockedDomain2, apiLockedDomain)

      val userBody =
        j"""{
        "id" : "lazy-bear",
        "screenName" : "lazy bear",
        "email" : "lazy.bear@forest.com"
        }"""

      mockServer.when(
        request()
          .withMethod("GET")
          .withPath("/users.json")
          .withHeader("X-Socrata-Host", apiLockedDomain.domainCname)
      ).respond(
        response()
          .withStatusCode(200)
          .withHeader("Content-Type", "application/json; charset=utf-8")
          .withBody(CompactJsonWriter.toString(userBody))
      )
      mockServer.when(
        request()
          .withMethod("GET")
          .withPath("/users/lazy-bear")
          .withHeader("X-Socrata-Host", lockedDomain.domainCname)
      ).respond(
        response()
          .withStatusCode(200)
          .withHeader("Content-Type", "application/json; charset=utf-8")
          .withBody(CompactJsonWriter.toString(userBody))
      )

      val res = domainClient.removeLockedDomainsForbiddenToUser(Some(apiLockedDomain), relevantDomains, None)
      res should be((None, Set(unlockedDomain2)))
    }

    "remove locked domains if the user has a good cookie, but no role on the locked domain (where domain is not the context)" in {
      val relevantDomains = Set(lockedDomain, doublyLockedDomain, unlockedDomain1)

      val authedUserBody =
        j"""{
        "id" : "boo-bear",
        "roleName" : "headBear",
        "rights" : [ "steal_honey", "scare_tourists"],
        "flags" : [ "admin" ]
        }"""

      val unauthedUserBody =
        j"""{
        "id" : "boo-bear"
        }"""

      mockServer.when(
        request()
          .withMethod("GET")
          .withPath("/users.json")
          .withHeader("X-Socrata-Host", lockedDomain.domainCname)
      ).respond(
        response()
          .withStatusCode(200)
          .withHeader("Content-Type", "application/json; charset=utf-8")
          .withBody(CompactJsonWriter.toString(authedUserBody))
      )
      mockServer.when(
        request()
          .withMethod("GET")
          .withPath("/users/boo-bear")
          .withHeader("X-Socrata-Host", lockedDomain.domainCname)
      ).respond(
        response()
          .withStatusCode(200)
          .withHeader("Content-Type", "application/json; charset=utf-8")
          .withBody(CompactJsonWriter.toString(authedUserBody))
      )
      mockServer.when(
        request()
          .withMethod("GET")
          .withPath("/users/boo-bear")
          .withHeader("X-Socrata-Host", doublyLockedDomain.domainCname)
      ).respond(
        response()
          .withStatusCode(200)
          .withHeader("Content-Type", "application/json; charset=utf-8")
          .withBody(CompactJsonWriter.toString(unauthedUserBody))
      )

      val res = domainClient.removeLockedDomainsForbiddenToUser(
        Some(lockedDomain), relevantDomains, Some("c=cookie"))
      res should be((Some(lockedDomain), Set(lockedDomain, unlockedDomain1)))
    }

    "return all the things if nothing is locked down" in {
      val noContextNoDomains = domainClient.removeLockedDomainsForbiddenToUser(None, Set.empty[Domain], None)
      noContextNoDomains should be((None, Set.empty[Domain]))

      val noDomains = domainClient.removeLockedDomainsForbiddenToUser(
        Some(unlockedDomain0), Set.empty[Domain], None)
      noDomains should be((Some(unlockedDomain0), Set.empty[Domain]))

      val noContext = domainClient.removeLockedDomainsForbiddenToUser(None, Set(unlockedDomain1), None)
      noContext should be((None, Set(unlockedDomain1)))

      val bothUnsharedContext = domainClient.removeLockedDomainsForbiddenToUser(
        Some(unlockedDomain0), Set(unlockedDomain2), None)
      bothUnsharedContext should be((Some(unlockedDomain0), Set(unlockedDomain2)))

      val bothSharedContext = domainClient.removeLockedDomainsForbiddenToUser(
        Some(unlockedDomain1),
        Set(unlockedDomain1, unlockedDomain2), None)
      bothSharedContext should be((Some(unlockedDomain1), Set(unlockedDomain1, unlockedDomain2)))

    }

    "return all the things if the user is logged in and has roles on all the domains" in {
      val relevantDomains = Set(lockedDomain, doublyLockedDomain)

      val userBody =
        j"""{
          "id" : "boo-bear",
          "roleName" : "headBear",
          "rights" : [ "steal_honey", "scare_tourists"],
          "flags" : [ "admin" ]
          }"""

      mockServer.when(
        request()
          .withMethod("GET")
          .withPath("/users.json")
          .withHeader("X-Socrata-Host", apiLockedDomain.domainCname)
      ).respond(
        response()
          .withStatusCode(200)
          .withHeader("Content-Type", "application/json; charset=utf-8")
          .withBody(CompactJsonWriter.toString(userBody))
      )
      mockServer.when(
        request()
          .withMethod("GET")
          .withPath("/users/boo-bear")
          .withHeader("X-Socrata-Host", lockedDomain.domainCname)
      ).respond(
        response()
          .withStatusCode(200)
          .withHeader("Content-Type", "application/json; charset=utf-8")
          .withBody(CompactJsonWriter.toString(userBody))
      )
      mockServer.when(
        request()
          .withMethod("GET")
          .withPath("/users/boo-bear")
          .withHeader("X-Socrata-Host", doublyLockedDomain.domainCname)
      ).respond(
        response()
          .withStatusCode(200)
          .withHeader("Content-Type", "application/json; charset=utf-8")
          .withBody(CompactJsonWriter.toString(userBody))
      )

      val res = domainClient.removeLockedDomainsForbiddenToUser(
        Some(apiLockedDomain), relevantDomains, Some("c=cookie"))
      res should be((Some(apiLockedDomain), relevantDomains))
    }
  }
}
