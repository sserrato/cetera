package com.socrata.cetera.search

import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.CompactJsonWriter
import org.mockserver.integration.ClientAndServer.startClientAndServer
import org.mockserver.matchers.Times
import org.mockserver.model.HttpRequest.request
import org.mockserver.model.HttpResponse.response
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, ShouldMatchers, WordSpec}

import com.socrata.cetera._
import com.socrata.cetera.types.{Domain, DomainSet}

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
      val (domainSet, _, _) = domainClient.findSearchableDomains(Some("petercetera.net"), None, true, None, None)
      domainSet.searchContext.get should be(expectedContext)
    }

    "return the domain if it exists among the given cnames : opendata-demo.socrata.com" in {
      val expectedContext = domains(1)
      val (domainSet, _, _) = domainClient.findSearchableDomains(
        Some("opendata-demo.socrata.com"), Some(Set("opendata-demo.socrata.com")), true, None, None)
      domainSet.searchContext.get should be(expectedContext)
    }

    "return all the unlocked customer domains if not given cnames" in {
      val unlockedDomains = Set(domains(0), domains(2), domains(3), domains(4))
      val (domainSet, _, _) = domainClient.findSearchableDomains(None, None, true, None, None)
      domainSet.domains should be(unlockedDomains)
    }

    "return all the unlocked domains among the given cnames if they exist" in {
      val expectedDomains = Set(domains(3), domains(4))
      val wantedDomains = Some(expectedDomains.map(d => d.domainCname))
      val (domainSet, _, _) = domainClient.findSearchableDomains(None, wantedDomains, true, None, None)
      domainSet.domains should be(expectedDomains)
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
          .withHeader(HeaderXSocrataHostKey, context.domainCname)
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

      val (domainSet, _, _) = domainClient.findSearchableDomains(Some(context.domainCname),
        Some(wantedCnames), true, Some("c=cookie"), None)
      domainSet.searchContext.get should be(context)
      domainSet.domains should be(wantedDomains)
    }

    "throw DomainNotFound exception when searchContext is missing" in {
      intercept[DomainNotFound] {
        domainClient.findSearchableDomains(Some("iamnotarealdomain.wat"), None, true, None, None)
      }
    }

    "throw DomainNotFound exception when searchContext is missing even if domains are found" in {
      intercept[DomainNotFound] {
        domainClient.findSearchableDomains(Some("iamnotarealdomain.wat"), Some(Set("dylan.demo.socrata.com")), true, None, None)
      }
    }

    "not throw DomainNotFound exception when searchContext is present" in {
      noException should be thrownBy {
        domainClient.findSearchableDomains(Some("dylan.demo.socrata.com"), None, true, None, None)
      }
    }

    "not throw DomainNotFound exception when domains are missing" in {
      noException should be thrownBy {
        domainClient.findSearchableDomains(None, Some(Set("iamnotarealdomain.wat")), true, None, None)
      }
    }
  }

  "removeLockedDomainsFromUnauthorizedUsers" should {
    "return None for a locked context if the user has no cookie" in {
      val (withoutDomains, _) = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(Set.empty[Domain], Some(lockedDomain)), None, None)
      withoutDomains should be(DomainSet.empty)

      val (withDomains, _) = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(Set(domains(1)), Some(lockedDomain)), None, None)
      withDomains should be(DomainSet(Set(domains(1)), None))
    }

    "return None for a locked context if the user has a bad cookie" in {
      mockServer.when(
        request()
          .withMethod("GET")
          .withPath("/users.json")
          .withHeader(HeaderXSocrataHostKey, apiLockedDomain.domainCname),
        Times.exactly(2)
      ).respond(
        response()
          .withStatusCode(401)
      )

      val (withoutDomains, _) = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(Set.empty[Domain], Some(apiLockedDomain)), Some("c=cookie"), None)
      withoutDomains should be(DomainSet.empty)

      val (withDomains, _) = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(Set(domains(1)), Some(apiLockedDomain)), Some("c=cookie"), None)
      withDomains should be(DomainSet(Set(domains(1)),None))
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
          .withHeader(HeaderXSocrataHostKey, doublyLockedDomain.domainCname),
        Times.exactly(2)
      ).respond(
        response()
          .withStatusCode(200)
          .withHeader("Content-Type", "application/json; charset=utf-8")
          .withBody(CompactJsonWriter.toString(userBody))
      )

      val (withoutDomains, _) = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(Set.empty[Domain], Some(doublyLockedDomain)), Some("c=cookie"), None)
      withoutDomains should be(DomainSet.empty)

      val (withDomains, _) = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(Set(domains(1)), Some(doublyLockedDomain)), Some("c=cookie"), None)
      withDomains should be(DomainSet(Set(domains(1)), None))
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
          .withHeader(HeaderXSocrataHostKey, lockedDomain.domainCname)
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
          .withHeader(HeaderXSocrataHostKey, lockedDomain.domainCname)
      ).respond(
        response()
          .withStatusCode(200)
          .withHeader("Content-Type", "application/json; charset=utf-8")
          .withBody(CompactJsonWriter.toString(userBody))
      )

      val (res, _) = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(Set(lockedDomain), Some(lockedDomain)), Some("c=cookie"), None)
      res should be(DomainSet(Set(lockedDomain), Some(lockedDomain)))
    }

    "remove locked domains if the user has no cookie" in {
      val relevantDomains = Set(unlockedDomain0, lockedDomain)
      val (withoutContext, _) = domainClient.removeLockedDomainsForbiddenToUser(DomainSet(relevantDomains, None), None, None)
      withoutContext should be(DomainSet(Set(unlockedDomain0), None))

      val (withContext, _) = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(relevantDomains, Some(unlockedDomain0)), None, None)
      withContext should be(DomainSet(Set(unlockedDomain0), Some(unlockedDomain0)))
    }

    "remove locked domains if the user has a bad cookie" in {
      val relevantDomains = Set(unlockedDomain0, doublyLockedDomain)

      mockServer.when(
        request()
          .withMethod("GET")
          .withPath("/users.json")
          .withHeader(HeaderXSocrataHostKey, unlockedDomain0.domainCname)
      ).respond(
        response()
          .withStatusCode(401)
      )

      val (res, _) = domainClient.removeLockedDomainsForbiddenToUser(DomainSet(relevantDomains, Some(unlockedDomain0)), None, None)
      res should be(DomainSet(Set(unlockedDomain0), Some(unlockedDomain0)))
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
          .withHeader(HeaderXSocrataHostKey, apiLockedDomain.domainCname)
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
          .withHeader(HeaderXSocrataHostKey, lockedDomain.domainCname)
      ).respond(
        response()
          .withStatusCode(200)
          .withHeader("Content-Type", "application/json; charset=utf-8")
          .withBody(CompactJsonWriter.toString(userBody))
      )

      val (res, _) = domainClient.removeLockedDomainsForbiddenToUser(DomainSet(relevantDomains, Some(apiLockedDomain)), None, None)
      res should be(DomainSet(Set(unlockedDomain2), None))
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
          .withHeader(HeaderXSocrataHostKey, lockedDomain.domainCname)
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
          .withHeader(HeaderXSocrataHostKey, lockedDomain.domainCname)
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
          .withHeader(HeaderXSocrataHostKey, doublyLockedDomain.domainCname)
      ).respond(
        response()
          .withStatusCode(200)
          .withHeader("Content-Type", "application/json; charset=utf-8")
          .withBody(CompactJsonWriter.toString(unauthedUserBody))
      )

      val (res, _) = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(relevantDomains, Some(lockedDomain)), Some("c=cookie"), None)
      res should be(DomainSet(Set(lockedDomain, unlockedDomain1), Some(lockedDomain)))
    }

    "return all the things if nothing is locked down" in {
      val (noContextNoDomains, _) = domainClient.removeLockedDomainsForbiddenToUser(DomainSet.empty, None, None)
      noContextNoDomains should be(DomainSet.empty)

      val (noDomains, _) = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(Set.empty[Domain], Some(unlockedDomain0)), None, None)
      noDomains should be(DomainSet(Set.empty[Domain], Some(unlockedDomain0)))

      val (noContext, _) = domainClient.removeLockedDomainsForbiddenToUser(DomainSet(Set(unlockedDomain1), None), None, None)
      noContext should be(DomainSet(Set(unlockedDomain1), None))

      val (bothUnsharedContext, _) = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(Set(unlockedDomain2), Some(unlockedDomain0)), None, None)
      bothUnsharedContext should be(DomainSet(Set(unlockedDomain2), Some(unlockedDomain0)))

      val (bothSharedContext, _) = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(Set(unlockedDomain1, unlockedDomain2), Some(unlockedDomain1)), None, None)
      bothSharedContext should be(DomainSet(Set(unlockedDomain1, unlockedDomain2), Some(unlockedDomain1)))

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
          .withHeader(HeaderXSocrataHostKey, apiLockedDomain.domainCname)
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
          .withHeader(HeaderXSocrataHostKey, lockedDomain.domainCname)
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
          .withHeader(HeaderXSocrataHostKey, doublyLockedDomain.domainCname)
      ).respond(
        response()
          .withStatusCode(200)
          .withHeader("Content-Type", "application/json; charset=utf-8")
          .withBody(CompactJsonWriter.toString(userBody))
      )

      val (res, _) = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(relevantDomains, Some(apiLockedDomain)), Some("c=cookie"), None)
      res should be(DomainSet(relevantDomains, Some(apiLockedDomain)))
    }
  }
}
