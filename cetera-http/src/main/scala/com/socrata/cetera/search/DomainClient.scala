package com.socrata.cetera.search

import com.rojoma.json.v3.codec.JsonDecode
import com.rojoma.json.v3.io.JsonReader
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchType}
import org.elasticsearch.index.query.QueryBuilders
import org.slf4j.LoggerFactory

import com.socrata.cetera._
import com.socrata.cetera.search.DomainAggregations._
import com.socrata.cetera.search.DomainFilters._
import com.socrata.cetera.types.{Domain, DomainCnameFieldType, QueryType}
import com.socrata.cetera.util.LogHelper

class DomainClient(val esClient: ElasticSearchClient, val indexAliasName: String) {
  val logger = LoggerFactory.getLogger(getClass)

  def fetch(id: Int): Option[Domain] = {
    logger.debug(s"fetching domain id $id")
    val res = esClient.client.prepareGet(indexAliasName, esDomainType, id.toString)
      .execute.actionGet
    Domain(res.getSourceAsString)
  }

  def find(cname: String): Option[Domain] = {
    val search = esClient.client.prepareSearch(indexAliasName).setTypes(esDomainType)
      .setQuery(QueryBuilders.matchPhraseQuery(DomainCnameFieldType.fieldName, cname))
    logger.debug(LogHelper.formatEsRequest(search))
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

  def odnSearch: Seq[Domain] = {
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

  def fetchOrAllCustomerDomains(domainIds: Set[Int]): Set[Domain] =
    domainIds.flatMap(fetch) match {
      case ds: Set[Domain] if ds.nonEmpty => ds
      case _ => odnSearch.toSet
    }

  def findOrAllCustomerDomains(domainCnames: Set[String]): Set[Domain] =
    domainCnames.flatMap(find) match {
      case ds: Set[Domain] if ds.nonEmpty => ds
      case _ => odnSearch.toSet
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
      domainCnames: Set[String],
      searchContext: Option[Domain],
      categories: Option[Set[String]],
      tags: Option[Set[String]],
      only: Option[Seq[String]])
    : SearchRequestBuilder = {
    val (domainsFilter, domainsAggregation) = {
      val contextMod = searchContext.exists(_.moderationEnabled)
      val (ids, mod, unmod, raOff) = calculateIdsAndModRAStatuses(findOrAllCustomerDomains(domainCnames))
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
