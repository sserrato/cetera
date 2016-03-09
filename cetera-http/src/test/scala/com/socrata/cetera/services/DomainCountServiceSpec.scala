package com.socrata.cetera.services

import com.rojoma.json.v3.ast.{JNumber, JString}
import org.scalatest.{BeforeAndAfterAll, Matchers, FunSuiteLike}

import com.socrata.cetera.{TestESData, TestESClient}
import com.socrata.cetera.search._
import com.socrata.cetera.types.Count

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
      Count("opendata-demo.socrata.com", 0),
      Count("petercetera.net", 4))
    val (res, _) = service.doAggregate(Map())
    res.results should contain theSameElementsAs expectedResults
  }

  test("only count publicly visible documents") {
    val (res, _) = service.doAggregate(Map())
    res.results.foreach { count =>
      // opendata-demo.socrata.com is not a customer domain, so all docs should be hidden
      if (count.thing == JString("opendata-demo.socrata.com")) {
        count.count should be(JNumber(0))
      }
    }
  }
}
