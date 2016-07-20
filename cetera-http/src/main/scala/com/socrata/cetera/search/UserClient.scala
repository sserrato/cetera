package com.socrata.cetera.search

import org.slf4j.LoggerFactory

import com.socrata.cetera._
import com.socrata.cetera.types._
import com.socrata.cetera.util.LogHelper
import com.socrata.cetera.search.UserQueries.userQuery

trait BaseUserClient {
  def fetch(id: String): Option[EsUser]
  def search(
      q: Option[String],
      role: Option[String],
      domain: Option[Domain],
      limit: Int, offset: Int)
    : (Seq[EsUser], Long)
}

class UserClient(esClient: ElasticSearchClient, indexAliasName: String) extends BaseUserClient {
  val logger = LoggerFactory.getLogger(getClass)

  def fetch(id: String): Option[EsUser] = {
    val get = esClient.client.prepareGet(indexAliasName, esUserType, id)
    logger.info("Elasticsearch request: " + get.request.toString)

    val res = get.execute.actionGet
    EsUser(res.getSourceAsString)
  }

  // TODO: setup user search pagination
  val maxLimit = 200 // Parity with Core for now
  def search(
      query: Option[String],
      role: Option[String],
      domain: Option[Domain],
      limit: Int = maxLimit,
      offset: Int = 0)
    : (Seq[EsUser], Long) = {

    val baseQuery = userQuery(query, role, domain)

    val req = esClient.client.prepareSearch(indexAliasName)
      .setTypes(esUserType)
      .setQuery(baseQuery)
      .setFrom(offset)
      .setSize(limit)
    logger.info(LogHelper.formatEsRequest(req))

    val res = req.execute.actionGet
    val timing = res.getTookInMillis

    val users = res.getHits.hits.flatMap { hit =>
      try { EsUser(hit.sourceAsString) }
      catch { case e: Exception =>
        logger.info(e.getMessage)
        None
      }
    }

    (users, timing)
  }
}
