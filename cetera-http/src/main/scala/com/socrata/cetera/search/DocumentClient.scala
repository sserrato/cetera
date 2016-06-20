package com.socrata.cetera.search

import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.index.query._
import org.elasticsearch.search.aggregations.AggregationBuilders

import com.socrata.cetera._
import com.socrata.cetera.search.DocumentAggregations._
import com.socrata.cetera.search.DocumentFilters.compositeFilter
import com.socrata.cetera.search.DocumentQueries.{chooseMatchQuery, compositeFilteredQuery}
import com.socrata.cetera.types._

trait BaseDocumentClient {
  def buildSearchRequest( // scalastyle:ignore parameter.number
      searchQuery: QueryType,
      domains: Set[Domain],
      domainMetadata: Option[Set[(String, String)]],
      searchContext: Option[Domain],
      categories: Option[Set[String]],
      tags: Option[Set[String]],
      datatypes: Option[Set[String]],
      user: Option[String],
      attribution: Option[String],
      parentDatasetId: Option[String],
      fieldBoosts: Map[CeteraFieldType with Boostable, Float],
      datatypeBoosts: Map[Datatype, Float],
      domainIdBoosts: Map[Int, Float],
      minShouldMatch: Option[String],
      slop: Option[Int],
      offset: Int,
      limit: Int,
      sortOrder: Option[String])
    : SearchRequestBuilder

  def buildCountRequest( // scalastyle:ignore parameter.number
      field: DocumentFieldType with Countable with Rawable,
      searchQuery: QueryType,
      domains: Set[Domain],
      domainMetadata: Option[Set[(String, String)]],
      searchContext: Option[Domain],
      categories: Option[Set[String]],
      tags: Option[Set[String]],
      datatypes: Option[Set[String]],
      user: Option[String],
      attribution: Option[String],
      parentDatasetId: Option[String])
    : SearchRequestBuilder

  def buildFacetRequest(domain: Option[Domain]): SearchRequestBuilder
}

class DocumentClient(
    esClient: ElasticSearchClient,
    domainClient: BaseDomainClient,
    indexAliasName: String,
    defaultTitleBoost: Option[Float],
    defaultMinShouldMatch: Option[String],
    scriptScoreFunctions: Set[ScriptScoreFunction])
  extends BaseDocumentClient {


  // Assumes validation has already been done
  //
  // Called by buildSearchRequest and buildCountRequest
  //
  // * Chooses query type to be used and constructs query with applicable boosts
  // * Applies function scores (typically views and score) with applicable domain boosts
  // * Applies filters (facets and searchContext-sensitive federation preferences)
 private def buildBaseRequest( // scalastyle:ignore parameter.number method.length
      searchQuery: QueryType,
      domains: Set[Domain],
      searchContext: Option[Domain],
      categories: Option[Set[String]],
      tags: Option[Set[String]],
      domainMetadata: Option[Set[(String, String)]],
      datatypes: Option[Set[String]],
      user: Option[String],
      attribution: Option[String],
      parentDatasetId: Option[String],
      fieldBoosts: Map[CeteraFieldType with Boostable, Float],
      datatypeBoosts: Map[Datatype, Float],
      domainIdBoosts: Map[Int, Float],
      minShouldMatch: Option[String],
      slop: Option[Int])
    : SearchRequestBuilder = {

    // Construct basic match query
    val matchQuery = chooseMatchQuery(searchQuery, searchContext, fieldBoosts, datatypeBoosts,
      minShouldMatch, slop, defaultTitleBoost, defaultMinShouldMatch)

    val idsAndModRAStatuses = domainClient.calculateIdsAndModRAStatuses(domains)

    // Wrap basic match query in filtered query for filtering
    val filteredQuery = compositeFilteredQuery(
      datatypes,
      user,
      attribution,
      parentDatasetId,
      domains,
      searchContext,
      categories,
      tags,
      domainMetadata,
      matchQuery,
      idsAndModRAStatuses)

    // Wrap filtered query in function score query for boosting
    val query = QueryBuilders.functionScoreQuery(filteredQuery)
    Boosts.applyScoreFunctions(query, scriptScoreFunctions)
    Boosts.applyDomainBoosts(query, domainIdBoosts)
    query.scoreMode("multiply").boostMode("replace")

    val preparedSearch = esClient.client
      .prepareSearch(indexAliasName)
      .setQuery(query)
      .setTypes(esDocumentType)

    preparedSearch
  }

  def buildSearchRequest( // scalastyle:ignore parameter.number
      searchQuery: QueryType,
      domains: Set[Domain],
      domainMetadata: Option[Set[(String, String)]],
      searchContext: Option[Domain],
      categories: Option[Set[String]],
      tags: Option[Set[String]],
      datatypes: Option[Set[String]],
      user: Option[String],
      attribution: Option[String],
      parentDatasetId: Option[String],
      fieldBoosts: Map[CeteraFieldType with Boostable, Float],
      datatypeBoosts: Map[Datatype, Float],
      domainIdBoosts: Map[Int, Float],
      minShouldMatch: Option[String],
      slop: Option[Int],
      offset: Int,
      limit: Int,
      sortOrder: Option[String])
    : SearchRequestBuilder = {

    val baseRequest = buildBaseRequest(
      searchQuery,
      domains,
      searchContext,
      categories,
      tags,
      domainMetadata,
      datatypes,
      user,
      attribution,
      parentDatasetId,
      fieldBoosts,
      datatypeBoosts,
      domainIdBoosts,
      minShouldMatch,
      slop
    )

    // WARN: Sort will totally blow away score if score isn't part of the sort
    // "Relevance" without a query can mean different things, so chooseSort decides
    val sort = sortOrder match {
      case Some(so) if so != "relevance" => Sorts.paramSortMap.get(so).get // will raise if invalid param got through
      case _ => Sorts.chooseSort(searchQuery, searchContext, categories, tags)
    }

    baseRequest
      .setFrom(offset)
      .setSize(limit)
      .addSort(sort)
  }

  def buildCountRequest( // scalastyle:ignore parameter.number
      field: DocumentFieldType with Countable with Rawable,
      searchQuery: QueryType,
      domains: Set[Domain],
      domainMetadata: Option[Set[(String, String)]],
      searchContext: Option[Domain],
      categories: Option[Set[String]],
      tags: Option[Set[String]],
      datatypes: Option[Set[String]],
      user: Option[String],
      attribution: Option[String],
      parentDatasetId: Option[String])
    : SearchRequestBuilder = {

    val aggregation = chooseAggregation(field)

    val baseRequest = buildBaseRequest(
      searchQuery,
      domains,
      searchContext,
      categories,
      tags,
      domainMetadata,
      datatypes,
      user,
      attribution,
      parentDatasetId,
      Map.empty,
      Map.empty,
      Map.empty,
      None,
      None
    )

    baseRequest
      .addAggregation(aggregation)
      .setSearchType("count")
      .setSize(0) // no docs, aggs only
  }

  def buildFacetRequest(domain: Option[Domain]): SearchRequestBuilder = {
    val aggSize = 0 // agg count unlimited
    val searchSize = 0 // no docs, aggs only

    val datatypeAgg = AggregationBuilders
      .terms("datatypes")
      .field(DatatypeFieldType.fieldName)
      .size(aggSize)

    val categoryAgg = AggregationBuilders
      .terms("categories")
      .field(DomainCategoryFieldType.rawFieldName)
      .size(aggSize)

    val tagAgg = AggregationBuilders
      .terms("tags")
      .field(DomainTagsFieldType.rawFieldName)
      .size(aggSize)

    val metadataAgg = AggregationBuilders
      .nested("metadata")
      .path(DomainMetadataFieldType.fieldName)
      .subAggregation(AggregationBuilders.terms("keys")
        .field(DomainMetadataFieldType.Key.rawFieldName)
        .size(aggSize)
        .subAggregation(AggregationBuilders.terms("values")
          .field(DomainMetadataFieldType.Value.rawFieldName)
          .size(aggSize)))

    val domainSpecificFilter = domain
      .map(d => compositeFilter(Set(d), None, None, None, None, None, None,
        domainClient.calculateIdsAndModRAStatuses(Set(d))))
      .getOrElse(FilterBuilders.matchAllFilter())

    val filteredAggs = AggregationBuilders
      .filter("domain_filter")
      .filter(domainSpecificFilter)
      .subAggregation(datatypeAgg)
      .subAggregation(categoryAgg)
      .subAggregation(tagAgg)
      .subAggregation(metadataAgg)

    val preparedSearch = esClient.client
      .prepareSearch(indexAliasName)
      .addAggregation(filteredAggs)
      .setSize(searchSize)

    preparedSearch
  }
}
