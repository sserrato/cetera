package com.socrata.cetera

import scala.collection.JavaConverters._

import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}

import com.socrata.cetera.search.ElasticSearchClient
import com.socrata.cetera.types._

class TestESDataSpec extends FunSuiteLike with Matchers with TestESData with BeforeAndAfterAll {
  val client: ElasticSearchClient = new TestESClient(testSuiteName)

  override protected def beforeAll(): Unit = {
    bootstrapData()
  }

  override protected def afterAll(): Unit = {
    removeBootstrapData()
    client.close()
  }

  test("aliases are bootstrapped") {
    val res = client.client.admin.indices.prepareGetAliases()
      .execute.actionGet
    val indexAliases = res.getAliases.asScala.map(ooc => ooc.key -> ooc.value.asScala.map(_.alias))
    indexAliases.size should be(1) // number of indices
    indexAliases.flatMap(as => as._2).size should be(1) // number of aliases
    indexAliases.find { case (_,as) => as.contains(testSuiteName) } should be('defined)
  }

  test("settings are bootstrapped") {
    val res = client.client.admin.indices.prepareGetSettings(testSuiteName)
      .execute.actionGet
    val settings = res.getIndexToSettings.asScala.map(ooc => ooc.key -> ooc.value)
    settings.size should be(1)
  }

  test("mappings are bootstrapped") {
    val res = client.client.admin().indices().prepareGetMappings()
      .execute.actionGet
    val mappings = res.getMappings.asScala.flatMap { ooc =>
      ooc.value.asScala.map { iom =>
        iom.key -> iom.value.sourceAsMap().asScala
      }
    }
    mappings.size should be(2)
  }

  test("test docs are bootstrapped") {
    val res = client.client.prepareSearch().execute.actionGet
    val numDocs = Datatypes.materialized.length + 4
    val numDomains = domains.length
    res.getHits.getTotalHits should be(numDocs + numDomains)
  }
}
