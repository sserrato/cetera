package com.socrata.cetera.search

import com.rojoma.json.v3.util.JsonUtil
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchType}
import org.elasticsearch.index.query.QueryBuilders
import org.slf4j.LoggerFactory

import com.socrata.cetera._
import com.socrata.cetera.search.DomainAggregations._
import com.socrata.cetera.search.DomainFilters._
import com.socrata.cetera.types.{Domain, DomainCnameFieldType, QueryType}
import com.socrata.cetera.util.{JsonDecodeException, LogHelper}

class DomainClient(val esClient: ElasticSearchClient, val indexAliasName: String) {
  val logger = LoggerFactory.getLogger(getClass)

  def fetch(id: Int): Option[Domain] = {
    logger.debug(s"fetching domain id $id")
    val res = esClient.client.prepareGet(indexAliasName, esDomainType, id.toString)
      .execute.actionGet
    Domain(res.getSourceAsString)
  }

  def find(cname: String): Option[Domain] = find(Set(cname)).headOption

  def find(cnames: Set[String]): Set[Domain] = {
    val query = QueryBuilders.boolQuery()
    cnames.foreach(s => query.should(QueryBuilders.matchPhraseQuery(DomainCnameFieldType.fieldName, s)))
    val search = esClient.client.prepareSearch(indexAliasName).setTypes(esDomainType)
      .setQuery(query)
    logger.debug(LogHelper.formatEsRequest(search))

    val res = search.execute.actionGet
    res.getHits.hits.flatMap { h =>
      JsonUtil.parseJson[Domain](h.sourceAsString) match {
        case Right(domain) => Some(domain)
        case Left(err) =>
          logger.error(err.english)
          throw new JsonDecodeException(err)
      }
    }.toSet
  }

  // if domain cname filter is provided limit to that scope, otherwise default to publicly visible domains
  def findRelevantDomains(searchContextCname: Option[String], domainCnames: Set[String]): Set[Domain] = {
    searchContextCname.foldLeft(domainCnames) { (b, x) => b + x } match {
      case cs: Set[String] if cs.nonEmpty => find(cs)
      case _ => customerDomainSearch.toSet
    }
  }

  // when query doesn't define domain filter, we assume all customer domains.
  private def customerDomainSearch: Seq[Domain] = {
    val search = esClient.client.prepareSearch(indexAliasName).setTypes(esDomainType)
      .setQuery(QueryBuilders.filteredQuery(
        QueryBuilders.matchAllQuery(),
        isCustomerDomainFilter)
      )
    logger.debug(LogHelper.formatEsRequest(search))
    val res = search.execute.actionGet
    res.getHits.hits.flatMap { h =>
      Domain(h.getSourceAsString)
    }
  }

  // (pre-)calculate domain moderation and R+A status
  def calculateIdsAndModRAStatuses(domains: Set[Domain]): (Set[Int], Set[Int], Set[Int], Set[Int]) = {
    val ids = domains.map(_.domainId)
    val mod = domains.collect { case d: Domain if d.moderationEnabled => d.domainId }
    val unmod = domains.collect { case d: Domain if !d.moderationEnabled => d.domainId }
    val raOff = domains.collect { case d: Domain if !d.routingApprovalEnabled => d.domainId }
    (ids, mod, unmod, raOff)
  }

  def buildCountRequest(
      searchQuery: QueryType,
      searchDomains: Set[Domain],
      searchContext: Option[Domain],
      categories: Option[Set[String]],
      tags: Option[Set[String]],
      only: Option[Seq[String]])
    : SearchRequestBuilder = {
    val (domainsFilter, domainsAggregation) = {
      val contextMod = searchContext.exists(_.moderationEnabled)
      val (ids, mod, unmod, raOff) = calculateIdsAndModRAStatuses(searchDomains)
      val dsFilter = if (ids.nonEmpty) domainIds(ids) else isCustomerDomainFilter
      val dsAggregation = domains(contextMod, mod, unmod, raOff)
      (dsFilter, dsAggregation)
    }
    esClient.client.prepareSearch(indexAliasName).setTypes(esDomainType)
      .setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), domainsFilter))
      .addAggregation(domainsAggregation)
      .setSearchType(SearchType.COUNT)
  }
}

class DomainNotFound(cname: String) extends NoSuchElementException(cname)
