package com.socrata.cetera.search

import org.scalatest.{BeforeAndAfterAll, ShouldMatchers, WordSpec}

import com.socrata.cetera.{TestESData, TestESClient}

class DomainClientSpec extends WordSpec with ShouldMatchers with TestESData with BeforeAndAfterAll {
  val client = new TestESClient("domainClient")
  val domainClient: DomainClient = new DomainClient(client)

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
        routingApprovalEnabled = false)
      val actualDomain = domainClient.find("petercetera.net")
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
        routingApprovalEnabled = false)
      val actualDomain = domainClient.find("opendata-demo.socrata.com")
      actualDomain.get should be(expectedDomain)
    }

    "return None if the domain does not exist" in {
      val expectedDomain = None
      val actualDomain = domainClient.find("hellcat.com")
      actualDomain should be(expectedDomain)
    }
  }
}
