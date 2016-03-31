package com.socrata.cetera.search

import org.scalatest.{BeforeAndAfterAll, ShouldMatchers, WordSpec}

import com.socrata.cetera.types.Domain
import com.socrata.cetera.{TestESClient, TestESData}

class DomainClientSpec extends WordSpec with ShouldMatchers with TestESData with BeforeAndAfterAll {
  val client = new TestESClient(testSuiteName)
  val domainClient: DomainClient = new DomainClient(client, testSuiteName)

  override protected def beforeAll(): Unit = {
    bootstrapData()
  }

  override protected def afterAll(): Unit = {
    removeBootstrapData()
    client.close()
  }

  "find" should {
    "return the domain if it exists : petercetera.net" in {
      val expectedDomain = Domain(
        isCustomerDomain = true,
        organization = Some(""),
        domainCname = "petercetera.net",
        domainId = 0,
        siteTitle = Some("Temporary URI"),
        moderationEnabled = false,
        routingApprovalEnabled = false,
        lockedDown = false,
        apiLockedDown = false)
      val (actualDomain, _) = domainClient.find("petercetera.net")
      actualDomain.get should be(expectedDomain)
    }

    "return the domain if it exists : opendata-demo.socrata.com" in {
      val expectedDomain = Domain(
        isCustomerDomain = false,
        organization = Some(""),
        domainCname = "opendata-demo.socrata.com",
        domainId = 1,
        siteTitle = Some("And other things"),
        moderationEnabled = true,
        routingApprovalEnabled = false,
        lockedDown = false,
        apiLockedDown = false)
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
      val expectedDomain = Domain(
        isCustomerDomain = true,
        organization = Some(""),
        domainCname = "dylan.demo.socrata.com",
        domainId = 4,
        siteTitle = Some("Temporary URI"),
        moderationEnabled = false,
        routingApprovalEnabled = false,
        lockedDown = false,
        apiLockedDown = false)

      val (actualDomain, _) = domainClient.find("dylan.demo.socrata.com")
      actualDomain shouldBe Some(expectedDomain)
    }
  }

  "findRelevantDomains" should {
    "throw DomainNotFound exception when searchContext is missing" in {
      intercept[DomainNotFound] {
        domainClient.findRelevantDomains(Some("iamnotarealdomain.wat"), None)
      }
    }

    "throw DomainNotFound exception when searchContext is missing even if domains are found" in {
      intercept[DomainNotFound] {
        domainClient.findRelevantDomains(Some("iamnotarealdomain.wat"), Some(Set("dylan.demo.socrata.com")))
      }
    }

    "not throw DomainNotFound exception when searchContext is present" in {
      noException should be thrownBy {
        domainClient.findRelevantDomains(Some("dylan.demo.socrata.com"), None)
      }
    }

    "not throw DomainNotFound exception when domains are missing" in {
      noException should be thrownBy {
        domainClient.findRelevantDomains(None, Some(Set("iamnotarealdomain.wat")))
      }
    }
  }
}
