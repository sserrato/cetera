package com.socrata.cetera.search

import org.elasticsearch.action.search.{SearchRequestBuilder, SearchType}
import org.elasticsearch.index.query.QueryBuilders
import org.slf4j.LoggerFactory

import com.socrata.cetera._
import com.socrata.cetera.auth.{CoreClient, User}
import com.socrata.cetera.errors.DomainNotFoundError
import com.socrata.cetera.search.DomainFilters.{idsFilter, isCustomerDomainFilter}
import com.socrata.cetera.types.{Domain, DomainCnameFieldType, DomainSet}
import com.socrata.cetera.util.LogHelper

trait BaseDomainClient {
  def fetch(id: Int): Option[Domain]

  def findSearchableDomains(
      searchContextCname: Option[String],
      domainCnames: Option[Set[String]],
      excludeLockedDomains: Boolean,
      user: Option[User],
      requestId: Option[String])
    : (DomainSet, Long)

  def buildCountRequest(domainSet: DomainSet, user: Option[User]): SearchRequestBuilder
}

class DomainClient(esClient: ElasticSearchClient, coreClient: CoreClient, indexAliasName: String)
  extends BaseDomainClient {

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

    val domains = res.getHits.hits.flatMap { hit =>
      try { Domain(hit.sourceAsString) }
      catch { case e: Exception =>
        logger.info(e.getMessage)
        None
      }
    }.toSet

    (domains, timing)
  }

  // This method returns a DomainSet including optional search context and list of Domains to search.
  // Each Domain will be visible to the requesting User or anonymous. If a Domain is locked-down and the
  // requesting User is not assigned a role it will be elided.
  // WARN: The search context may come back as None. This is to avoid leaking locked domains through the context.
  //       But if a search context was given this effectively pretends no context was given.
  def removeLockedDomainsForbiddenToUser(
      domainSet: DomainSet,
      loggedInUser: Option[User],
      requestid: Option[String])
    : DomainSet = {
    val context = domainSet.searchContext
    val contextLocked = context.exists(_.isLocked)
    val (lockedDomains, unlockedDomains) = domainSet.domains.partition(_.isLocked)

    if (!contextLocked && lockedDomains.isEmpty) {
      domainSet
    } else {
      val viewableContext = context.filter(c => !contextLocked || loggedInUser.exists(_.canViewLockedDownCatalog))
      loggedInUser match {
        case Some(u) =>
          val viewableLockedDomains =
            lockedDomains.filter { d =>
              val (userId, _) = coreClient.fetchUserById(d.domainCname, u.id, requestid)
              userId.exists(_.canViewLockedDownCatalog)
            }
          DomainSet(unlockedDomains ++ viewableLockedDomains, viewableContext)
        case None => // user is not logged in and thus can see no locked data
          DomainSet(unlockedDomains, viewableContext)
      }
    }
  }


  // if domain cname filter is provided limit to that scope, otherwise default to publicly visible domains
  // also looks up the search context and throws if it cannot be found
  // If lock-down is a concern, use the 'findSearchableDomains' method in lieu of this one.
  def findDomainSet(searchContextCname: Option[String], domainCnames: Option[Set[String]])
  : (DomainSet, Long) = {
    // We want to fetch all relevant domains (search context and relevant domains) in a single query
    // NOTE: the searchContext may be present as both the context and in the relevant domains
    val (foundDomains, timings) = domainCnames match {
      case Some(cnames) => find(cnames ++ searchContextCname)
      case None => customerDomainSearch
    }
    val searchContextDomain = searchContextCname.flatMap(cname => foundDomains.find(_.domainCname == cname))
    val domains = domainCnames match {
      case Some(cnames) => foundDomains.filter(d => cnames.contains(d.domainCname))
      case None => foundDomains
    }

    // If a searchContext is specified and we can't find it, we have to bail
    searchContextCname.foreach(c => if (searchContextDomain.isEmpty) throw new DomainNotFoundError(c))

    (DomainSet(domains, searchContextDomain), timings)
  }

  def findSearchableDomains(
      searchContextCname: Option[String],
      domainCnames: Option[Set[String]],
      excludeLockedDomains: Boolean,
      user: Option[User],
      requestId: Option[String])
    : (DomainSet, Long) = {
    val (domainSet, timings) = findDomainSet(searchContextCname, domainCnames)
    if (excludeLockedDomains) {
      val restrictedDomainSet =
        removeLockedDomainsForbiddenToUser(domainSet, user, requestId)
      (restrictedDomainSet, timings)
    } else {
      (domainSet, timings)
    }
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

    val domains = res.getHits.hits.flatMap { hit =>
      try { Domain(hit.getSourceAsString) }
      catch { case e: Exception =>
        logger.info(e.getMessage)
        None
      }
    }.toSet

    (domains, timing)
  }

  // NOTE: I do not currently honor counting according to parameters
  def buildCountRequest(domainSet: DomainSet, user: Option[User]): SearchRequestBuilder = {
    val domainFilter = idsFilter(domainSet.allIds)
    val aggregation = DomainAggregations.domains(domainSet, user)

    esClient.client.prepareSearch(indexAliasName).setTypes(esDomainType)
      .setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), domainFilter))
      .addAggregation(aggregation)
      .setSearchType(SearchType.COUNT)
      .setSize(0) // no docs, aggs only
  }
}
