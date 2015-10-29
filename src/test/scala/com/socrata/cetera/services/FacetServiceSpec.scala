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

    val categories = facets.find(_.facet == "categories").map(_.values).getOrElse(fail())
    println(categories)
    categories.find(_.value == "") shouldNot be('defined)
    domainCategories.distinct.filter(_.nonEmpty).foreach { cat =>
      categories.find(_.value == cat) should be('defined)
    }

    val tags = facets.find(_.facet == "tags").map(_.values).getOrElse(fail())
    tags.find(_.value == "") shouldNot be('defined)
    domainTags.flatten.distinct.filter(_.nonEmpty).foreach { tag =>
      tags.find(_.value == tag) should be('defined)
    }

    domainMetadata.flatMap(_.keys).distinct.foreach { key =>
      facets.find(_.facet == key) should be('defined)
    }
  }
}
