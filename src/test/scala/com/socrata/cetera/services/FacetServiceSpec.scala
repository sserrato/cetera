package com.socrata.cetera.services

import com.socrata.cetera.search.{TestESClient, TestESData}
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}

class FacetServiceSpec extends FunSuiteLike with Matchers with TestESData with BeforeAndAfterAll {
  val client = new TestESClient(testSuiteName)
  val service = new FacetService(Some(client))

  override protected def beforeAll(): Unit = {
    bootstrapData()
  }

  override protected def afterAll(): Unit = {
    removeBootstrapData()
    client.close()
  }

  test("retrieve all domain facets") {
    val (facets, timings) = service.doAggregate("")

    timings.searchMillis should be('defined)

    val categories = facets.getOrElse("categories", fail())
    domainCategories.distinct.foreach { cat =>
      categories.find(fc => fc.facet == cat) should be('defined)
    }

    val tags = facets.getOrElse("tags", fail())
    domainTags.flatten.distinct.foreach { tag =>
      tags.find(fc => fc.facet == tag) should be('defined)
    }

    val metadata = facets.getOrElse("metadata", fail())
    domainMetadata.flatMap(_.keys).distinct.foreach { key =>
      metadata.find(fc => fc.facet == key) should be('defined)
    }
  }
}
