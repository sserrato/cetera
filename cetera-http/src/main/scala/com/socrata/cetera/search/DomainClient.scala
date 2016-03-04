package com.socrata.cetera.search

import com.rojoma.json.v3.codec.{DecodeError, JsonDecode}
import com.rojoma.json.v3.io.JsonReader
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, JsonKeyStrategy, JsonUtil, Strategy}
import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.index.query.QueryBuilders
import org.slf4j.LoggerFactory

import com.socrata.cetera._
import com.socrata.cetera.types.{DomainCnameFieldType, QueryType}
import com.socrata.cetera.util.LogHelper

@JsonKeyStrategy(Strategy.Underscore)
case class Domain(isCustomerDomain: Boolean,
                  organization: Option[String],
                  domainCname: String,
                  domainId: Int,
                  siteTitle: Option[String],
                  moderationEnabled: Boolean,
                  routingApprovalEnabled: Boolean)

object Domain {
  implicit val jCodec = AutomaticJsonCodecBuilder[Domain]
  val logger = LoggerFactory.getLogger(getClass)

  case class JsonDecodeException(err: DecodeError) extends RuntimeException {
    override def getMessage: String = err.english
  }

  def apply(source: String): Option[Domain] = {
    Option(source).flatMap { s =>
      JsonUtil.parseJson[Domain](s) match {
        case Right(domain) => Some(domain)
        case Left(err) =>
          logger.error(err.english)
          throw new JsonDecodeException(err)
      }
    }
  }
}

class DomainClient(val esClient: ElasticSearchClient) {
  val logger = LoggerFactory.getLogger(getClass)

  def fetch(id: Int): Option[Domain] = fetch(id.toString)
  def fetch(id: String): Option[Domain] = {
    Indices.flatMap { idx =>
      val res = esClient.client.prepareGet(idx, esDomainType, id)
        .execute.actionGet
      Domain(res.getSourceAsString)
    }.headOption
  }

  def find(cname: String): Option[Domain] = {
    val search = esClient.client.prepareSearch(Indices: _*).setTypes(esDomainType)
      .setQuery(QueryBuilders.matchPhraseQuery(DomainCnameFieldType.fieldName, cname))
    logger.info(LogHelper.formatEsRequest(Indices, search))
    val res = search.execute.actionGet
    val hits = res.getHits.hits
    hits.length match {
      case 0 => None
      case n: Int =>
        val domainAsJvalue = JsonReader.fromString(hits(0).getSourceAsString)
        val domainDecode = JsonDecode.fromJValue[Domain](domainAsJvalue)
        domainDecode match {
          case Right(domain) => Some(domain)
          case Left(err) =>
            logger.error(err.english)
            throw new Exception(s"Error decoding $esDomainType $cname")
        }
    }
  }

  def buildCountRequest(
      searchQuery: QueryType,
      domains: Set[String],
      searchContext: Option[Domain],
      categories: Option[Set[String]],
      tags: Option[Set[String]],
      only: Option[Seq[String]])
    : SearchRequestBuilder = {
    esClient.client.prepareSearch(Indices: _*).setTypes(esDomainType)
      .addAggregation(Aggregations.domains)
      .setSearchType("count")
  }
}
