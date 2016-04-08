package com.socrata.cetera.search

import org.scalatest.{BeforeAndAfterAll, ShouldMatchers, WordSpec}

import com.socrata.cetera.types.Domain
import com.socrata.cetera.{TestHttpClient, TestCoreClient, TestESClient, TestESData}

class DomainClientSpec extends WordSpec with ShouldMatchers with TestESData with BeforeAndAfterAll {
  val client = new TestESClient(testSuiteName)
  val httpClient = new TestHttpClient()  // Remember to close() me!!
  val coreClient = new TestCoreClient(httpClient, 8030)  // Remember to close() me!!
  val domainClient = new DomainClient(client, coreClient, testSuiteName)

  override protected def beforeAll(): Unit = {
    bootstrapData()
  }

  override protected def afterAll(): Unit = {
    removeBootstrapData()
    client.close()
    httpClient.close()
  }

  "find" should {
    "return the domain if it exists : petercetera.net" in {
      val expectedDomain = Domain(
        domainId = 0,
        domainCname = "petercetera.net",
        siteTitle = Some("Temporary URI"),
        organization = Some("org"),
        isCustomerDomain = true,
        moderationEnabled = false,
        routingApprovalEnabled = false,
        lockedDown = false,
        apiLockedDown = false)
      val (actualDomain, _) = domainClient.find("petercetera.net")
      actualDomain.get should be(expectedDomain)
    }

    "return the domain if it exists : opendata-demo.socrata.com" in {
      val expectedDomain = Domain(
        domainId = 1,
        domainCname = "opendata-demo.socrata.com",
        siteTitle = Some("And other things"),
        organization = Some("org"),
        isCustomerDomain = false,
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
        domainId = 4,
        domainCname = "dylan.demo.socrata.com",
        siteTitle = Some("Skid Row"),
        organization = Some("Hair Metal Bands"),
        isCustomerDomain = true,
        moderationEnabled = false,
        routingApprovalEnabled = true,
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
