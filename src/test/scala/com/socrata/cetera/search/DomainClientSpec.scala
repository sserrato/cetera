package com.socrata.cetera.search

import org.scalatest.{BeforeAndAfterAll, ShouldMatchers, WordSpec}

class DomainClientSpec extends WordSpec with ShouldMatchers  with TestESData with BeforeAndAfterAll {
  val client = new TestESClient("domainClient")
  val domainClient: DomainClient = new DomainClient(client)

  override protected def beforeAll(): Unit = {
    bootstrapData()
  }

  override protected def afterAll(): Unit = {
    removeBootstrapData()
    client.close()
  }

  "getDomain" should {
    "return the right domain if it exists" in {
      val expectedDomain = Domain(
        isCustomerDomain = true,
        organization = Some(""),
        domainCname = "petercetera.net",
        domainId = 0,
        siteTitle = Some("Temporary URI"),
        moderationEnabled = false,
        routingApprovalEnabled = true)
      val actualDomain = domainClient.find("petercetera.net")

      actualDomain.get should be(expectedDomain)
    }

    "return None if the domain does not exist" in {
      val expectedDomain = None
      val actualDomain = domainClient.find("hellcat.com")

      actualDomain should be(expectedDomain)
    }
  }
}
