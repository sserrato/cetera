package com.socrata.cetera.search

import com.rojoma.json.v3.codec.JsonDecode
import com.rojoma.json.v3.io.JsonReader
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, Strategy, JsonKeyStrategy}
import org.elasticsearch.index.query._
import org.slf4j.LoggerFactory

import com.socrata.cetera._

@JsonKeyStrategy(Strategy.Underscore)
case class Domain(isCustomerDomain: Boolean,
                  organization: Option[String],
                  domainCname: String,
                  siteTitle: Option[String],
                  moderationEnabled: Boolean,
                  routingApprovalEnabled: Boolean)

object Domain {
  implicit val jCodec = AutomaticJsonCodecBuilder[Domain]
}

class DomainClient(val esClient: ElasticSearchClient) {

  val logger = LoggerFactory.getLogger(getClass)
  val baseRequest = esClient.client.prepareSearch(Indices: _*)

  def getDomain(cname: String): Option[Domain] = {
    val query = baseRequest.setQuery(QueryBuilders.idsQuery(esDomainType).addIds(cname))
    val res = query.execute.actionGet
    val hits = res.getHits.hits
    hits.length match {
      case 0 => None
      case n: Int =>
        val domainAsJvalue = JsonReader.fromString(hits(0).getSourceAsString())
        val domainDecode = JsonDecode.fromJValue[Domain](domainAsJvalue)
        domainDecode match {
          case Right(domain) => Some(domain)
          case Left(err) =>
            logger.error(err.english)
            throw new Exception(s"Error decoding domain $cname")
        }
    }
  }
}
