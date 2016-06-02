package com.socrata.cetera.search

import com.rojoma.json.v3.util.JsonUtil
import org.elasticsearch.index.query.QueryBuilders
import org.slf4j.LoggerFactory

import com.socrata.cetera._
import com.socrata.cetera.types._
import com.socrata.cetera.util.{JsonDecodeException, LogHelper}

trait BaseUserClient {
  def fetch(id: String): Option[User]
  def search(q: Option[String]): (Set[User], Long)
}

class UserClient(esClient: ElasticSearchClient, indexAliasName: String) extends BaseUserClient {
  val logger = LoggerFactory.getLogger(getClass)

  def fetch(id: String): Option[User] = {
    val get = esClient.client.prepareGet(indexAliasName, esUserType, id)
    logger.info("Elasticsearch request: " + get.request.toString)

    val res = get.execute.actionGet
    User(res.getSourceAsString)
  }

  def search(query: Option[String]): (Set[User], Long) = {
    val baseQuery = query match {
      case None => QueryBuilders.matchAllQuery()
      case Some(q) =>
        QueryBuilders
          .queryStringQuery(q)
          .field(ScreenName.fieldName)
          .field(ScreenName.rawFieldName)
          .field(Email.fieldName)
          .field(Email.rawFieldName)
          .autoGeneratePhraseQueries(true)
    }
    val req = esClient.client.prepareSearch(indexAliasName)
      .setTypes(esUserType)
      .setQuery(baseQuery)
    logger.info(LogHelper.formatEsRequest(req))

    val res = req.execute.actionGet
    val timing = res.getTookInMillis
    val users = res.getHits.hits.flatMap { h =>
      JsonUtil.parseJson[User](h.sourceAsString) match {
        case Right(u) => Some(u)
        case Left(err) =>
          logger.error(err.english)
          throw new JsonDecodeException(err)
      }
    }.toSet

    (users, timing)
  }
}
