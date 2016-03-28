package com.socrata.cetera.search

import com.rojoma.json.v3.util.JsonUtil
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchType}
import org.elasticsearch.index.query.QueryBuilders
import org.slf4j.LoggerFactory

import com.socrata.cetera._
import com.socrata.cetera.search.DomainFilters.{domainIdsFilter, isCustomerDomainFilter}
import com.socrata.cetera.types.{Domain, DomainCnameFieldType, QueryType}
import com.socrata.cetera.util.{JsonDecodeException, LogHelper}

class DomainClient(val esClient: ElasticSearchClient, val indexAliasName: String) {
  val logger = LoggerFactory.getLogger(getClass)

  def fetch(id: Int): Option[Domain] = {
    val get = esClient.client.prepareGet(indexAliasName, esDomainType, id.toString)
    logger.info("Elasticsearch request: " + get.request.toString)

    val res = get.execute.actionGet
    Domain(res.getSourceAsString)
  }

  def find(cname: String): (Option[Domain], Long) = {
    val (domains, timing) = find(Set(cname))
    (domains.headOption, timing)
  }

  def find(cnames: Set[String]): (Set[Domain], Long) = {
    val query = QueryBuilders.termsQuery(DomainCnameFieldType.rawFieldName, cnames.toList: _*)

    val search = esClient.client.prepareSearch(indexAliasName).setTypes(esDomainType)
      .setQuery(query)
      .setSize(cnames.size)

    logger.info(LogHelper.formatEsRequest(search))

    val res = search.execute.actionGet
    val timing = res.getTookInMillis
    val domains = res.getHits.hits.flatMap { h =>
      JsonUtil.parseJson[Domain](h.sourceAsString) match {
        case Right(domain) => Some(domain)
        case Left(err) =>
          logger.error(err.english)
          throw new JsonDecodeException(err)
      }
    }.toSet

    (domains, timing)
  }

  // if domain cname filter is provided limit to that scope, otherwise default to publicly visible domains
  // also looks up the search context and throws if it cannot be found
  def findRelevantDomains(
      searchContextCname: Option[String],
      domainCnames: Option[Set[String]])
    : (Option[Domain], Option[Set[Domain]], Long) = {

    // We want to fetch all relevant domains (search context and query domains) in a single query
    val (foundDomains, timings) = domainCnames match {
      case Some(cnames) => find(domainCnames.getOrElse(Set.empty[String]) ++ searchContextCname)
      case None => customerDomainSearch
    }

    // If a searchContext is specified and we can't find it, we have to bail
    val searchContextDomain = searchContextCname.flatMap(cname => foundDomains.find(_.domainCname == cname))
    searchContextCname.foreach { searchContext =>
      if (!foundDomains.exists(_.domainCname == searchContext)) {
        throw new DomainNotFound(searchContext)
      }
    }

    // None means none were specified; Set.empty means none could be found
    val foundQueryDomains = domainCnames match {
      case Some(cnames) => Some(cnames.flatMap(cname => foundDomains.find(_.domainCname == cname)))
      case None => Some(foundDomains)
    }

    (searchContextDomain, foundQueryDomains, timings)
  }

  // TODO: handle unlimited domain count with aggregation or scan+scroll query
  // if we get 42 thousand customer domains before addressing this, most of us will probably be millionaires anyway.
  private val customerDomainSearchSize = 42000

  // when query doesn't define domain filter, we assume all customer domains.
  private def customerDomainSearch: (Set[Domain], Long) = {
    val search = esClient.client.prepareSearch(indexAliasName).setTypes(esDomainType)
      .setQuery(QueryBuilders.filteredQuery(
        QueryBuilders.matchAllQuery(),
        isCustomerDomainFilter)
      )
      .setSize(customerDomainSearchSize)
    logger.info(LogHelper.formatEsRequest(search))

    val res = search.execute.actionGet
    val timing = res.getTookInMillis
    val domains = res.getHits.hits.flatMap { h =>
      Domain(h.getSourceAsString)
    }.toSet
    (domains, timing)
  }

  // (pre-)calculate domain moderation and R+A status
  def calculateIdsAndModRAStatuses(domains: Set[Domain]): (Set[Int], Set[Int], Set[Int], Set[Int]) = {
    val ids = domains.map(_.domainId)
    val mod = domains.collect { case d: Domain if d.moderationEnabled => d.domainId }
    val unmod = domains.collect { case d: Domain if !d.moderationEnabled => d.domainId }
    val raOff = domains.collect { case d: Domain if !d.routingApprovalEnabled => d.domainId }
    (ids, mod, unmod, raOff)
  }

  // NOTE: I do not currently honor counting according to parameters
  def buildCountRequest(
      domains: Option[Set[Domain]],
      searchContext: Option[Domain])
    : SearchRequestBuilder = {

    val contextModerated = searchContext.exists(_.moderationEnabled)

    val (domainIds,
         moderatedDomainIds,
         unmoderatedDomainIds,
         routingApprovalDisabledDomainIds) = domains match {
      case Some(ds) => calculateIdsAndModRAStatuses(ds)
      case None => (Set.empty[Int], Set.empty[Int], Set.empty[Int], Set.empty[Int])
    }

    val domainFilter = domains match {
      case Some(ds) => domainIdsFilter(domainIds)
      case None => isCustomerDomainFilter
    }

    val aggregation = DomainAggregations.domains(
      contextModerated,
      moderatedDomainIds,
      unmoderatedDomainIds,
      routingApprovalDisabledDomainIds
    )

    esClient.client.prepareSearch(indexAliasName).setTypes(esDomainType)
      .setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), domainFilter))
      .addAggregation(aggregation)
      .setSearchType(SearchType.COUNT)
      .setSize(0) // no docs, aggs only
  }
}

// Should throw when Search Context is not found
class DomainNotFound(cname: String) extends NoSuchElementException(cname)
