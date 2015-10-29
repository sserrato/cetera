package com.socrata.cetera.services

import com.socrata.cetera.search.{ElasticSearchClient, TestESClient, TestESData}
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}

class FacetValueServiceSpec extends FunSuiteLike with Matchers with TestESData with BeforeAndAfterAll {
  val client: ElasticSearchClient = new TestESClient(testSuiteName)
  val service = new FacetValueService(Some(client))

  override protected def beforeAll(): Unit = {
    bootstrapData()
  }

  override protected def afterAll(): Unit = {
    removeBootstrapData()
    client.close()
  }

  test("retrieve all domain facet values") {
    val (values, timings) = service.doListValues("", None)

    timings.searchMillis should be('defined)

    val metadataValues = values.flatMap(kvp => kvp._2)
    domainMetadata.flatMap(m => m.values).distinct.foreach { v =>
      metadataValues.find(_.value == v) should be('defined)
    }
  }
}
