package com.socrata.cetera.services

import com.rojoma.json.v3.ast.{JNumber, JString}
import org.scalatest.{BeforeAndAfterAll, Matchers, FunSuiteLike}

import com.socrata.cetera.{TestESData, TestESClient}
import com.socrata.cetera.search._
import com.socrata.cetera.types.Count
import com.socrata.cetera.util.Params

class DomainCountServiceSpec extends FunSuiteLike with Matchers with BeforeAndAfterAll with TestESData {
  val client: ElasticSearchClient = new TestESClient(testSuiteName)
  val domainClient: DomainClient = new DomainClient(client, testSuiteName)
  val service: DomainCountService = new DomainCountService(domainClient)

  override protected def beforeAll(): Unit = {
    bootstrapData()
  }

  override protected def afterAll(): Unit = {
    client.close()
  }

  test("count domains") {
    val expectedResults = List(
      Count("annabelle.island.net", 0),
      Count("blue.org", 1),
      // opendata-demo.socrata.com is not a customer domain, so the domain and all docs should be hidden
      // Count("opendata-demo.socrata.com", 0),
      Count("petercetera.net", 4))
    val (res, _) = service.doAggregate(Map(
      Params.context -> "petercetera.net",
      Params.filterDomains -> "petercetera.net,opendata-demo.socrata.com,blue.org,annabelle.island.net")
      .mapValues(Seq(_)))
    res.results should contain theSameElementsAs expectedResults
  }
}
