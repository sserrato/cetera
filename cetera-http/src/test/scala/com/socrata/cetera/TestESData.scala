package com.socrata.cetera

import scala.io.Source

import com.rojoma.json.v3.util.JsonUtil

import com.socrata.cetera.search.ElasticSearchClient
import com.socrata.cetera.types._
import com.socrata.cetera.util.ElasticsearchBootstrap


trait TestESData extends TestESDomains with TestESUsers {
  val client: ElasticSearchClient
  val testSuiteName: String = getClass.getSimpleName.toLowerCase

  val docs = {
    // there is no difference in these two types of docs except for the means by which
    // we used to construct them in commit 1442a6b08.
    val series1DocFiles = (0 to 10).map(i => s"/views/fxf-$i.json")
    val series2DocFiles = (1 to 9).map(i => s"/views/zeta-000$i.json")

    val series1Docs = series1DocFiles.map { f =>
      val source = Source.fromInputStream(getClass.getResourceAsStream(f)).getLines().mkString("\n")
      Document(source).get
    }
    val series2Docs = series2DocFiles.map { f =>
      val source = Source.fromInputStream(getClass.getResourceAsStream(f)).getLines().mkString("\n")
      Document(source).get
    }
    series1Docs.toList ++ series2Docs.toList
  }

  def bootstrapData(): Unit = {
    ElasticsearchBootstrap.ensureIndex(client, "yyyyMMddHHmm", testSuiteName)

    // load domains
    domains.foreach { d =>
      client.client.prepareIndex(testSuiteName, esDomainType)
        .setSource(JsonUtil.renderJson[Domain](d))
        .setId(d.domainId.toString)
        .setRefresh(true)
        .execute.actionGet
    }

    // load users
    users.foreach { u =>
      client.client.prepareIndex(testSuiteName, esUserType)
        .setSource(JsonUtil.renderJson[EsUser](u))
        .setId(u.id)
        .setRefresh(true)
        .execute.actionGet
    }

    // load data
    docs.foreach { d =>
      client.client.prepareIndex(testSuiteName, esDocumentType)
        .setParent(d.socrataId.domainId.toString)
        .setSource(JsonUtil.renderJson(d))
        .setRefresh(true)
        .execute.actionGet
    }
  }

  def removeBootstrapData(): Unit = {
    client.client.admin().indices().prepareDelete(testSuiteName)
      .execute.actionGet
  }
}

