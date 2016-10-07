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
import com.socrata.cetera.types.DomainSet

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
  val unlockedCustomerDomain = domains(0)
  val unlockedNonCustomerDomain = domains(1)
  val unlockedCustomerDomain2 = domains(2)
  val lockedNonCustomerDomain = domains(6)
  val apiLockedDomain = domains(7)
  val lockedCustomerDomain = domains(8)
  val unlockedDomains = domains.filter(d => !d.isLocked)
  val unlockedCustomerDomains = domains.filter(d => d.isCustomerDomain && !d.isLocked)

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

  def prepUserAuth(role: String): Option[User] = {
    val userBody =
      j"""{
        "id" : "boo-bear",
        "roleName" : $role
      }"""

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
    User(userBody)
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

  "finding the context from findSearchableDomains when no cnames are given (and we rely on customer domain search)" should {
    "return the context if it's unlocked and no auth is given" in {
      val (domainSet, _) = domainClient.findSearchableDomains(Some(unlockedCustomerDomain.domainCname), None, None, true, None, None)
      domainSet.searchContext.get should be(unlockedCustomerDomain)
    }

    "return None for the context if it's locked and no auth is given" in {
      val (domainSet, _) = domainClient.findSearchableDomains(Some(lockedCustomerDomain.domainCname), None, None, true, None, None)
      domainSet.searchContext should be(None)
    }

    "return the context if it's locked and auth is given for the context" in {
      val user = prepUserAuth("editor")
      val (domainSet, _) = domainClient.findSearchableDomains(
        Some(lockedCustomerDomain.domainCname), Some(lockedCustomerDomain.domainCname), None, true, user, None)

      domainSet.searchContext.get should be(lockedCustomerDomain)
    }

    "return None for the context if it's locked and auth is given, but it ain't for the context" in {
      val user = prepUserAuth("editor")
      val authingDomain = unlockedCustomerDomain2
      val (domainSet, _) = domainClient.findSearchableDomains(
        Some(lockedCustomerDomain.domainCname), Some(authingDomain.domainCname), None, true, user, None)

      domainSet.searchContext should be(None)
    }
  }

  "finding the extended host from findSearchableDomains when no cnames are given (and we rely on customer domain search)" should {
    "return the extended host regardless of state if auth isn't given" in {
      val (lockedCustomerDomainSet, _) = domainClient.findSearchableDomains(
        None, Some(lockedCustomerDomain.domainCname), None, true, None, None)
      val (lockedNonCustomerDomainSet, _) = domainClient.findSearchableDomains(
        None, Some(lockedNonCustomerDomain.domainCname), None, true, None, None)
      val (unlockedCustomerDomainSet, _) = domainClient.findSearchableDomains(
        None, Some(unlockedCustomerDomain.domainCname), None, true, None, None)
      val (unlockedNonCustomerDomainSet, _) = domainClient.findSearchableDomains(
        None, Some(unlockedNonCustomerDomain.domainCname), None, true, None, None)

      lockedCustomerDomainSet.extendedHost.get should be(lockedCustomerDomain)
      lockedNonCustomerDomainSet.extendedHost.get should be(lockedNonCustomerDomain)
      unlockedCustomerDomainSet.extendedHost.get should be(unlockedCustomerDomain)
      unlockedNonCustomerDomainSet.extendedHost.get should be(unlockedNonCustomerDomain)
    }

    "return the extended host regardless of state if auth is given" in {
      val user = prepUserAuth("administrator")

      val (lockedCustomerDomainSet, _) = domainClient.findSearchableDomains(
        None, Some(lockedCustomerDomain.domainCname), None, true, user, None)
      val (lockedNonCustomerDomainSet, _) = domainClient.findSearchableDomains(
        None, Some(lockedNonCustomerDomain.domainCname), None, true, user, None)
      val (unlockedCustomerDomainSet, _) = domainClient.findSearchableDomains(
        None, Some(unlockedCustomerDomain.domainCname), None, true, user, None)
      val (unlockedNonCustomerDomainSet, _) = domainClient.findSearchableDomains(
        None, Some(unlockedNonCustomerDomain.domainCname), None, true, user, None)

      lockedCustomerDomainSet.extendedHost.get should be(lockedCustomerDomain)
      lockedNonCustomerDomainSet.extendedHost.get should be(lockedNonCustomerDomain)
      unlockedCustomerDomainSet.extendedHost.get should be(unlockedCustomerDomain)
      unlockedNonCustomerDomainSet.extendedHost.get should be(unlockedNonCustomerDomain)
    }
  }

  "finding the domains to search from findSearchableDomains when no cnames are given (and we rely on customer domain search)" should {
    "return only unlocked customer domains if auth isn't given" in {
      val (domainSet, _) = domainClient.findSearchableDomains(None, None, None, true, None, None)
      domainSet.domains should contain theSameElementsAs(unlockedCustomerDomains)
    }

    "return unlocked customer domains + locked customer domains if auth is given for the locked domain" in {
      val expectedDomains = unlockedCustomerDomains ++ Some(lockedCustomerDomain)
      val user = prepUserAuth("editor")
      val (domainSet, _) = domainClient.findSearchableDomains(None, Some(lockedCustomerDomain.domainCname), None, true, user, None)

      domainSet.domains should contain theSameElementsAs(expectedDomains)
    }

    "return only unlocked customer domains if auth is given but it ain't for any locked domain" in {
      val user = prepUserAuth("editor")
      val (domainSet, _) = domainClient.findSearchableDomains(None, Some(lockedNonCustomerDomain.domainCname), None, true, user, None)

      domainSet.domains should contain theSameElementsAs(unlockedCustomerDomains)
    }
  }

  "finding the context from findSearchableDomains when cnames are given" should {
    val domainsToSearch = Some(domains.map(_.domainCname).toSet)

    "return the context if it's unlocked and no auth is given" in {
      val (domainSet, _) = domainClient.findSearchableDomains(Some(unlockedCustomerDomain.domainCname), None, domainsToSearch, true, None, None)
      domainSet.searchContext.get should be(unlockedCustomerDomain)
    }

    "return None for the context if it's locked and no auth is given" in {
      val (domainSet, _) = domainClient.findSearchableDomains(Some(lockedCustomerDomain.domainCname), None, domainsToSearch, true, None, None)
      domainSet.searchContext should be(None)
    }

    "return the context if it's locked and auth is given for the context" in {
      val user = prepUserAuth("editor")
      val (domainSet, _) = domainClient.findSearchableDomains(
        Some(lockedCustomerDomain.domainCname), Some(lockedCustomerDomain.domainCname), domainsToSearch, true, user, None)

      domainSet.searchContext.get should be(lockedCustomerDomain)
    }

    "return None for the context if it's locked and auth is given, but it ain't for the context" in {
      val user = prepUserAuth("editor")
      val authingDomain = unlockedCustomerDomain2
      val (domainSet, _) = domainClient.findSearchableDomains(
        Some(lockedCustomerDomain.domainCname), Some(authingDomain.domainCname), domainsToSearch, true, user, None)

      domainSet.searchContext should be(None)
    }
  }

  "finding the extended host from findSearchableDomains when cnames are given" should {
    val domainsToSearch = Some(domains.map(_.domainCname).toSet)

    "return the extended host regardless of state if auth isn't given" in {
      val (lockedCustomerDomainSet, _) = domainClient.findSearchableDomains(
        None, Some(lockedCustomerDomain.domainCname), domainsToSearch, true, None, None)
      val (lockedNonCustomerDomainSet, _) = domainClient.findSearchableDomains(
        None, Some(lockedNonCustomerDomain.domainCname), domainsToSearch, true, None, None)
      val (unlockedCustomerDomainSet, _) = domainClient.findSearchableDomains(
        None, Some(unlockedCustomerDomain.domainCname), domainsToSearch, true, None, None)
      val (unlockedNonCustomerDomainSet, _) = domainClient.findSearchableDomains(
        None, Some(unlockedNonCustomerDomain.domainCname), domainsToSearch, true, None, None)

      lockedCustomerDomainSet.extendedHost.get should be(lockedCustomerDomain)
      lockedNonCustomerDomainSet.extendedHost.get should be(lockedNonCustomerDomain)
      unlockedCustomerDomainSet.extendedHost.get should be(unlockedCustomerDomain)
      unlockedNonCustomerDomainSet.extendedHost.get should be(unlockedNonCustomerDomain)
    }

    "return the extended host regardless of state if auth is given" in {
      val user = prepUserAuth("administrator")

      val (lockedCustomerDomainSet, _) = domainClient.findSearchableDomains(
        None, Some(lockedCustomerDomain.domainCname), domainsToSearch, true, user, None)
      val (lockedNonCustomerDomainSet, _) = domainClient.findSearchableDomains(
        None, Some(lockedNonCustomerDomain.domainCname), domainsToSearch, true, user, None)
      val (unlockedCustomerDomainSet, _) = domainClient.findSearchableDomains(
        None, Some(unlockedCustomerDomain.domainCname), domainsToSearch, true, user, None)
      val (unlockedNonCustomerDomainSet, _) = domainClient.findSearchableDomains(
        None, Some(unlockedNonCustomerDomain.domainCname), domainsToSearch, true, user, None)

      lockedCustomerDomainSet.extendedHost.get should be(lockedCustomerDomain)
      lockedNonCustomerDomainSet.extendedHost.get should be(lockedNonCustomerDomain)
      unlockedCustomerDomainSet.extendedHost.get should be(unlockedCustomerDomain)
      unlockedNonCustomerDomainSet.extendedHost.get should be(unlockedNonCustomerDomain)
    }
  }

  "finding the domains to search from findSearchableDomains when cnames are given" should {
    val domainsToSearch = Some(domains.map(_.domainCname).toSet)

    "return only unlocked domains if auth isn't given" in {
      val (domainSet, _) = domainClient.findSearchableDomains(None, None, domainsToSearch, true, None, None)
      domainSet.domains should contain theSameElementsAs(unlockedDomains)
    }

    "return unlocked domains + locked domains if auth is given for the locked domain" in {
      val expectedDomains = unlockedDomains ++ Some(lockedCustomerDomain)
      val user = prepUserAuth("editor")
      val (domainSet, _) = domainClient.findSearchableDomains(None, Some(lockedCustomerDomain.domainCname), domainsToSearch, true, user, None)

      domainSet.domains should contain theSameElementsAs(expectedDomains)
    }

    "return only unlocked domains if auth is given but it ain't for any locked domain" in {
      val user = prepUserAuth("editor")
      val (domainSet, _) = domainClient.findSearchableDomains(None, Some(unlockedCustomerDomain.domainCname), domainsToSearch, true, user, None)

      domainSet.domains should contain theSameElementsAs(unlockedDomains)
    }
  }

  "findSearchableDomains" should {
    "return the context, extended host and domains even if they are all the same thing: opendata-demo.socrata.com" in {
      val domain = domains(1)
      val (domainSet, _) = domainClient.findSearchableDomains(
        Some(domain.domainCname), Some(domain.domainCname), Some(Set(domain.domainCname)), true, None, None)

      domainSet.searchContext.get should be(domain)
      domainSet.extendedHost.get should be(domain)
      domainSet.domains should be(Set(domain))
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
        DomainSet(searchContext = Some(lockedNonCustomerDomain)), None, None)
      withoutDomains should be(DomainSet())

      val withDomains = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(Set(domains(1)), Some(lockedNonCustomerDomain)), None, None)
      withDomains should be(DomainSet(domains = Set(domains(1))))
    }

    "return None for a locked context if the user has no role" in {
      val user = User("lazy-bear", Some(unlockedCustomerDomain), roleName = None, rights = None, flags = None)

      val withoutDomains = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(searchContext = Some(lockedCustomerDomain)), Some(user), None)
      withoutDomains should be(DomainSet())

      val withDomains = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(Set(domains(1)), Some(lockedCustomerDomain)), Some(user), None)
      withDomains should be(DomainSet(domains = Set(domains(1))))
    }

    "return the locked down context if the user is logged in and has a role" in {
      val user = prepUserAuth("publisher")
      val res = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(Set(lockedNonCustomerDomain), Some(lockedNonCustomerDomain), Some(lockedNonCustomerDomain)), user, None)
      res should be(DomainSet(Set(lockedNonCustomerDomain), Some(lockedNonCustomerDomain), Some(lockedNonCustomerDomain)))
    }

    "remove locked domains if the user has no cookie" in {
      val relevantDomains = Set(unlockedCustomerDomain, lockedNonCustomerDomain)
      val withoutContext = domainClient.removeLockedDomainsForbiddenToUser(DomainSet(domains = relevantDomains), None, None)
      withoutContext should be(DomainSet(domains = Set(unlockedCustomerDomain)))

      val withContext = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(relevantDomains, Some(unlockedCustomerDomain), Some(unlockedCustomerDomain)), None, None)
      withContext should be(DomainSet(Set(unlockedCustomerDomain), Some(unlockedCustomerDomain), Some(unlockedCustomerDomain)))
    }

    "remove locked domains if the user has a bad cookie" in {
      val relevantDomains = Set(unlockedCustomerDomain, lockedCustomerDomain)
      val res = domainClient.removeLockedDomainsForbiddenToUser(DomainSet(relevantDomains, Some(unlockedCustomerDomain)), None, None)
      res should be(DomainSet(Set(unlockedCustomerDomain), Some(unlockedCustomerDomain)))
    }

    "remove locked domains if the user has a good cookie, but no role on the locked domain (where domain is the context)" in {
      val relevantDomains = Set(unlockedCustomerDomain2, apiLockedDomain)
      val user = prepUserAuth("")
      val res = domainClient.removeLockedDomainsForbiddenToUser(DomainSet(relevantDomains, Some(apiLockedDomain), Some(apiLockedDomain)), user, None)
      res should be(DomainSet(domains = Set(unlockedCustomerDomain2), extendedHost = Some(apiLockedDomain)))
    }

    "remove locked domains if the user has a good cookie, but no role on the locked domain (where domain is not the context)" in {
      val relevantDomains = Set(unlockedCustomerDomain, lockedCustomerDomain)
      val user = prepUserAuth("")
      val res = domainClient.removeLockedDomainsForbiddenToUser(DomainSet(relevantDomains, None, Some(unlockedCustomerDomain)), user, None)
      res should be(DomainSet(domains = Set(unlockedCustomerDomain), extendedHost = Some(unlockedCustomerDomain)))
    }

    "return all the things if nothing is locked down" in {
      val noContextNoDomains = domainClient.removeLockedDomainsForbiddenToUser(DomainSet(), None, None)
      noContextNoDomains should be(DomainSet())

      val noDomains = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(searchContext = Some(unlockedCustomerDomain)), None, None)
      noDomains should be(DomainSet(searchContext = Some(unlockedCustomerDomain)))

      val noContext = domainClient.removeLockedDomainsForbiddenToUser(DomainSet(domains = Set(unlockedNonCustomerDomain)), None, None)
      noContext should be(DomainSet(domains = Set(unlockedNonCustomerDomain)))

      val bothUnsharedContext = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(Set(unlockedCustomerDomain2), Some(unlockedCustomerDomain)), None, None)
      bothUnsharedContext should be(DomainSet(Set(unlockedCustomerDomain2), Some(unlockedCustomerDomain)))

      val bothSharedContext = domainClient.removeLockedDomainsForbiddenToUser(
        DomainSet(Set(unlockedNonCustomerDomain, unlockedCustomerDomain2), Some(unlockedNonCustomerDomain)), None, None)
      bothSharedContext should be(DomainSet(Set(unlockedNonCustomerDomain, unlockedCustomerDomain2), Some(unlockedNonCustomerDomain)))

    }
  }
}
