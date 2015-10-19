package com.socrata.cetera.services

import com.socrata.cetera._
import com.socrata.cetera.search.{TestESClient, TestESData}
import com.socrata.cetera.util.Timings
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}

import scala.collection.JavaConverters._

class FacetServiceSpec extends FunSuiteLike with Matchers with TestESData with BeforeAndAfterAll {
  val client = new TestESClient
  val service = new FacetService(Some(client))

  override protected def beforeAll(): Unit = {
    bootstrapData()
  }

  override protected def afterAll(): Unit = {
    removeBootstrapData()
    client.close()
  }

  test("aliases are bootstrapped") {
    val res = client.client.admin().indices().prepareGetAliases()
      .execute.actionGet
    val aliases = res.getAliases.asScala.map(ooc => ooc.key -> ooc.value.asScala.map(_.alias))
    aliases.size should be(Indices.length)
    Indices.foreach { index =>
      aliases.find { case (_,as) => as.contains(index) } should be('defined)
    }
  }

  test("settings are bootstrapped") {
    val res = client.client.admin().indices().prepareGetSettings()
      .execute.actionGet
    val settings = res.getIndexToSettings.asScala.map(ooc => ooc.key -> ooc.value)
    settings.size should be(Indices.length)
    Indices.foreach { index =>
      val ss = settings.find { case (i,_) => i.endsWith(index) }
      ss should be('defined)
//      println(ss)
    }
  }

  test("mappings are bootstrapped") {
    val res = client.client.admin().indices().prepareGetMappings()
      .execute.actionGet
    val mappings = res.getMappings.asScala.map { ooc =>
      ooc.key -> ooc.value.asScala.map { iom =>
        iom.key -> iom.value.sourceAsMap().asScala
      }
    }
    mappings.size should be(Indices.length)
    Indices.foreach { index =>
      val ms = mappings.find { case (i,_) => i.endsWith(index) }
      ms should be('defined)
//      println(ms)
    }
  }

  test("test docs are bootstrapped") {
    val res = client.client.prepareSearch().execute.actionGet
    res.getHits.getTotalHits should be(Indices.length)
  }

  test("retrieve all domain facets") {
    val (facets, timings) = service.doAggregate("", Timings.now())

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
