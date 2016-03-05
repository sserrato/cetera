package com.socrata.cetera.services

import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}

import com.socrata.cetera.search._
import com.socrata.cetera.types.{Datatypes, FacetCount, ValueCount}
import com.socrata.cetera.{TestESClient, TestESData}

class FacetServiceSpec extends FunSuiteLike with Matchers with TestESData with BeforeAndAfterAll {
  val client: ElasticSearchClient = new TestESClient(testSuiteName)
  val domainClient: DomainClient = new DomainClient(client, testSuiteName)
  val documentClient: DocumentClient = new DocumentClient(client, domainClient, testSuiteName, None, None, Set.empty)
  val service: FacetService = new FacetService(documentClient, domainClient)

  override protected def beforeAll(): Unit = {
    bootstrapData()
  }

  override protected def afterAll(): Unit = {
    removeBootstrapData()
    client.close()
  }

  test("retrieve all domain facets") {
    val (datatypes, categories, tags, facets) = domainCnames.map { cname =>
      val (facets, timings) = service.doAggregate(cname)
      timings.searchMillis should be('defined)

      val datatypes = facets.find(_.facet == "datatypes").map(_.values).getOrElse(fail())
      val categories = facets.find(_.facet == "categories").map(_.values).getOrElse(fail())
      val tags = facets.find(_.facet == "tags").map(_.values).getOrElse(fail())
      (datatypes, categories, tags, facets)
    }.foldLeft((Seq.empty[ValueCount], Seq.empty[ValueCount], Seq.empty[ValueCount], Seq.empty[FacetCount])) {
      (b, x) => (b._1 ++ x._1, b._2 ++ x._2, b._3 ++ x._3, b._4 ++ x._4)
    }

    Datatypes.materialized.foreach { dt =>
      datatypes.find(_.value == dt.singular) should be('defined)
    }

    categories.find(_.value == "") shouldNot be('defined)
    domainCategories.distinct.filter(_.nonEmpty).foreach { cat =>
      categories.find(_.value == cat) should be('defined)
    }

    tags.find(_.value == "") shouldNot be('defined)
    domainTags.flatten.distinct.filter(_.nonEmpty).foreach { tag =>
      tags.find(_.value == tag) should be('defined)
    }

    domainMetadata.flatMap(_.keys).distinct.foreach { key =>
      val facet = facets.find(_.facet == key)
      facet should be('defined)
      facet.get.count should be(facet.get.values.map(_.count).sum)
    }
  }
}
