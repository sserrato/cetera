package com.socrata.cetera.util

import scala.io.Source

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.util.JsonUtil
import org.elasticsearch.indices.IndexAlreadyExistsException
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import com.socrata.cetera._
import com.socrata.cetera.search.ElasticSearchClient

object ElasticsearchBootstrap {
  private def indexSettings: String = {
    val s = Source.fromInputStream(getClass.getResourceAsStream("/esSettings.json")).getLines().mkString("\n")
    val sj = JsonUtil.parseJson[JValue](s) match {
      case Left(e) => throw new JsonDecodeException(e)
      case Right(j) => j
    }
    sj.dyn.settings.!.toString()
  }

  private def datatypeMappings(datatype: String): String = {
    val s = Source.fromInputStream(getClass.getResourceAsStream("/esMappings.json")).getLines().mkString("\n")
    val sj = JsonUtil.parseJson[JValue](s) match {
      case Left(e) => throw new JsonDecodeException(e)
      case Right(j) => j
    }
    val mappings = sj.dyn.mappings
    val typeMapping = datatype match {
      case s: String if s == "document" => mappings.document
      case s: String if s == "domain" => mappings.domain
      case s: String if s == "user" => mappings.user
    }
    typeMapping.!.toString()
  }

  def ensureIndex(client: ElasticSearchClient,
                  indexBootstrapDatetimeFormatPattern: String,
                  indexAliasName: String): Unit = {
    val logger = LoggerFactory.getLogger(getClass)
    if (!client.indexExists) {
      try {
        val index = DateTime.now.toString(indexBootstrapDatetimeFormatPattern) + "_" + indexAliasName

        logger.info(s"creating index $index")
        client.client.admin.indices.prepareCreate(index)
          .setSettings(indexSettings)
          .execute.actionGet

        client.client.admin.indices.preparePutMapping(index)
          .setType(esDomainType)
          .setSource(datatypeMappings(esDomainType))
          .execute.actionGet

        client.client.admin.indices.preparePutMapping(index)
          .setType(esDocumentType)
          .setSource(datatypeMappings(esDocumentType))
          .execute.actionGet

        client.client.admin.indices.preparePutMapping(index)
          .setType(esUserType)
          .setSource(datatypeMappings(esUserType))
          .execute.actionGet

        logger.info(s"aliasing $indexAliasName -> $index")
        client.client.admin.indices.prepareAliases
          .addAlias(index, indexAliasName)
          .execute.actionGet

      } catch {
        case e: IndexAlreadyExistsException =>
          logger.info(s"actually that index ($indexAliasName) already exists!")
      }
    }
  }
}
