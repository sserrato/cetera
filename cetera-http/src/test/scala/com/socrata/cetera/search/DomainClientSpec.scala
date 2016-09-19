package com.socrata.cetera.search

import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.CompactJsonWriter
import org.mockserver.integration.ClientAndServer.startClientAndServer
import org.mockserver.model.HttpRequest.request
import org.mockserver.model.HttpResponse.response
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, ShouldMatchers, WordSpec}

import com.socrata.cetera._
import com.socrata.cetera.auth.User
import com.socrata.cetera.errors.DomainNotFoundError
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
      val (domainSet, _) = domainClient.findSearchableDomains(Some("petercetera.net"), None, None, true, None, None)
      domainSet.searchContext.get should be(expectedContext)
    }

    "return the domain if it exists among the given cnames : opendata-demo.socrata.com" in {
      val expectedContext = domains(1)
      val (domainSet, _) = domainClient.findSearchableDomains(
        Some("opendata-demo.socrata.com"), None, Some(Set("opendata-demo.socrata.com")), true, None, None)
      domainSet.searchContext.get should be(expectedContext)
    }

    "return the extended host if it exists among the given cnames : opendata-demo.socrata.com" in {
      val expectedHost = domains(1)
      val (domainSet, _) = domainClient.findSearchableDomains(None,
        Some("opendata-demo.socrata.com"), Some(Set("opendata-demo.socrata.com")), true, None, None)
      domainSet.extendedHost.get should be(expectedHost)
    }

    "return the context, extended host and domains even if they are all the same thing: opendata-demo.socrata.com" in {
      val domain = domains(1)
      val (domainSet, _) = domainClient.findSearchableDomains(
        Some(domain.domainCname), Some(domain.domainCname), Some(Set(domain.domainCname)), true, None, None)

      domainSet.searchContext.get should be(domain)
      domainSet.extendedHost.get should be(domain)
      domainSet.domains should be(Set(domain))
    }

    "return all the unlocked customer domains if not given cnames, context or extendhost" in {
      val unlockedDomains = Set(domains(0), domains(2), domains(3), domains(4))
      val (domainSet, _) = domainClient.findSearchableDomains(None, None, None, true, None, None)
      domainSet.domains should be(unlockedDomains)
    }

    "return context and extendhost if given in addition to all the unlocked customer domains if not given cnames" in {
      val unlockedDomains = Set(domains(0), domains(2), domains(3), domains(4))
      val expectedContext = domains(1) // not a customer domain, but unlocked
      val expectedHost = domains(5) // also not a customer domain, but unlocked

      val (domainSet, _) = domainClient.findSearchableDomains(
        Some(expectedContext.domainCname), Some(expectedHost.domainCname), None, true, None, None)

      domainSet.domains should be(unlockedDomains)
      domainSet.searchContext.get should be(expectedContext)
      domainSet.extendedHost.get should be(expectedHost)
    }

    "return context and extendhost if given in addition to all the customer domains (locked or unlocked) if not given cnames but user is authed" in {
      val unlockedDomainsForAuthedUsers = Set(domains(0), domains(2), domains(3), domains(4), domains(7), domains(8))
      val expectedContext = domains(7) // is a customer domain, but locked
      val expectedHost = domains(5)  // is not a customer domain, but unlocked
      val userBody =
        j"""{
          "id" : "boo-bear",
          "roleName" : "editor"
        }"""
      val user = User(userBody)

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

      val (domainSet, _) = domainClient.findSearchableDomains(
        Some(expectedContext.domainCname), Some(expectedHost.domainCname), None, true, user, None)

      domainSet.domains should be(unlockedDomainsForAuthedUsers)
      domainSet.searchContext.get should be(expectedContext)
      domainSet.extendedHost.get should be(expectedHost)
    }

    "return all the unlocked domains among the given cnames if they exist" in {
      val expectedDomains = Set(domains(3), domains(4))
      val wantedDomains = Some(expectedDomains.map(d => d.domainCname))
      val (domainSet, _) = domainClient.findSearchableDomains(None, None, wantedDomains, true, None, None)
      domainSet.domains should be(expectedDomains)
    }

    "return all the requested domains (locked or unlocked) if a good cookie is passed in" in {
      val context = domains(8)
      val wantedDomains = Set(domains(0), domains(6), domains(8))
      val wantedCnames = wantedDomains.map(_.domainCname)

      val userBody =
        j"""{
          "id" : "boo-bear",
          "roleName" : "editor"
        }"""
      val user = User(userBody)

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

      val (domainSet, _) = domainClient.findSearchableDomains(Some(context.domainCname), None,
        Some(wantedCnames), excludeLockedDomains = true, user, None)
      domainSet.searchContext.get should be(context)
      domainSet.domains should be(wantedDomains)
    }

    "throw DomainNotFound exception when searchContext is missing" in {
      intercept[DomainNotFoundError] {
        domainClient.findSearchableDomains(Some("iamnotarealdomain.wat"), None, None, true, None, None)
      }
    }

    "throw DomainNotFound exception when searchContext is missing even if domains are found" in {
      intercept[DomainNotFoundError] {
        domainClient.findSearchableDomains(Some("iamnotarealdomain.wat"), None,
          Some(Set("dylan.demo.socrata.com")), true, None, None)
      }
    }

    "not throw DomainNotFound exception when searchContext is present" in {
      noException should be thrownBy {
        domainClient.findSearchableDomains(Some("dylan.demo.socrata.com"), None, None, true, None, None)
      }
    }

    "not throw DomainNotFound exception when domains are missing" in {
      noException should be thrownBy {
        domainClient.findSearchableDomains(None, None, Some(Set("iamnotarealdomain.wat")), true, None, None)
      }
    }
  }

  "removeLockedDomainsFromUnauthorizedUsers" should {
    "return None for a locked context if the user has no cookie" in {
      val withoutDomains = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(searchContext = Some(lockedDomain)), None, None)
      withoutDomains should be(DomainSet())

      val withDomains = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(Set(domains(1)), Some(lockedDomain)), None, None)
      withDomains should be(DomainSet(domains = Set(domains(1))))
    }

    "return None for a locked context if the user has no role" in {
      val user = User("lazy-bear", None, roleName = None, rights = None, flags = None)

      val withoutDomains = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(searchContext = Some(doublyLockedDomain)), Some(user), None)
      withoutDomains should be(DomainSet())

      val withDomains = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(Set(domains(1)), Some(doublyLockedDomain)), Some(user), None)
      withDomains should be(DomainSet(domains = Set(domains(1))))
    }

    "return the locked down context if the user is logged in and has a role" in {
      val userBody =
        j"""{
          "id" : "boo-bear",
          "roleName" : "headBear",
          "rights" : [ "steal_honey", "scare_tourists"],
          "flags" : [ "admin" ]
        }"""
      val user = User(userBody)

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

      val res = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(Set(lockedDomain), Some(lockedDomain)), user, None)
      res should be(DomainSet(Set(lockedDomain), Some(lockedDomain)))
    }

    "remove locked domains if the user has no cookie" in {
      val relevantDomains = Set(unlockedDomain0, lockedDomain)
      val withoutContext = domainClient.removeLockedDomainsForbiddenToUser(DomainSet(domains = relevantDomains), None, None)
      withoutContext should be(DomainSet(domains = Set(unlockedDomain0)))

      val withContext = domainClient.removeLockedDomainsForbiddenToUser(
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

      val res = domainClient.removeLockedDomainsForbiddenToUser(DomainSet(relevantDomains, Some(unlockedDomain0)), None, None)
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

      val res = domainClient.removeLockedDomainsForbiddenToUser(DomainSet(relevantDomains, Some(apiLockedDomain)), None, None)
      res should be(DomainSet(domains = Set(unlockedDomain2)))
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
      val authedUser = User(authedUserBody)

      val unauthedUserBody =
        j"""{
          "id" : "boo-bear"
        }"""

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

      val res = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(relevantDomains, Some(lockedDomain)), authedUser, None)
      res should be(DomainSet(Set(lockedDomain, unlockedDomain1), Some(lockedDomain)))
    }

    "return all the things if nothing is locked down" in {
      val noContextNoDomains = domainClient.removeLockedDomainsForbiddenToUser(DomainSet(), None, None)
      noContextNoDomains should be(DomainSet())

      val noDomains = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(searchContext = Some(unlockedDomain0)), None, None)
      noDomains should be(DomainSet(searchContext = Some(unlockedDomain0)))

      val noContext = domainClient.removeLockedDomainsForbiddenToUser(DomainSet(domains = Set(unlockedDomain1)), None, None)
      noContext should be(DomainSet(domains = Set(unlockedDomain1)))

      val bothUnsharedContext = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(Set(unlockedDomain2), Some(unlockedDomain0)), None, None)
      bothUnsharedContext should be(DomainSet(Set(unlockedDomain2), Some(unlockedDomain0)))

      val bothSharedContext = domainClient.removeLockedDomainsForbiddenToUser(
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
      val user = User(userBody)

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

      val res = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(relevantDomains, Some(apiLockedDomain)), user, None)
      res should be(DomainSet(relevantDomains, Some(apiLockedDomain)))
    }
  }
}
