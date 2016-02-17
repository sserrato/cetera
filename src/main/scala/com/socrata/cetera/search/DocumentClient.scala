package com.socrata.cetera.search

import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.index.query._
import org.elasticsearch.search.aggregations.AggregationBuilders

import com.socrata.cetera.{Indices, esDocumentType}
import com.socrata.cetera.types._

object DocumentClient {
  def apply(
      esClient: ElasticSearchClient,
      defaultTitleBoost: Option[Float],
      defaultMinShouldMatch: Option[String],
      scriptScoreFunctions: Set[ScriptScoreFunction])
    : DocumentClient = {

    new DocumentClient(
      esClient,
      defaultTitleBoost,
      defaultMinShouldMatch,
      scriptScoreFunctions
    )
  }
}

class DocumentClient(
    esClient: ElasticSearchClient,
    defaultTitleBoost: Option[Float],
    defaultMinShouldMatch: Option[String],
    scriptScoreFunctions: Set[ScriptScoreFunction]) {

  // This query is complex, as it generates two queries that are then combined
  // into a single query. By default, the must match clause enforces a term match
  // such that one or more of the query terms must be present in at least one of the
  // fields specified. The optional minimum_should_match constraint applies to this
  // clause. The should clause is intended to give a subset of retrieved results boosts
  // based on:
  //
  //   1. better phrase matching in the case of multiterm queries
  //   2. matches in particular fields (specified in fieldBoosts)
  //
  // The scores are then averaged together by ES with a defacto score of 0 for a should
  // clause if it does not in fact match any documents. See the ElasticSearch
  // documentation here:
  //
  //   https://www.elastic.co/guide/en/elasticsearch/guide/current/proximity-relevance.html
  def generateSimpleQuery(
      queryString: String,
      fieldBoosts: Map[CeteraFieldType with Boostable, Float],
      datatypeBoosts: Map[Datatype, Float],
      minShouldMatch: Option[String],
      slop: Option[Int])
    : BaseQueryBuilder = {

    // Query #1: terms (must)
    val matchTerms = SundryBuilders.multiMatch(queryString, MultiMatchQueryBuilder.Type.CROSS_FIELDS)

    // Apply minShouldMatch if present
    minShouldMatch.foreach(SundryBuilders.addMinMatchConstraint(matchTerms, _))

    // Query #2: phrase (should)
    val matchPhrase = SundryBuilders.multiMatch(queryString, MultiMatchQueryBuilder.Type.PHRASE)

    // If no slop is specified, we do not set a default
    slop.foreach(SundryBuilders.addSlopParam(matchPhrase, _))

    // Add any optional field boosts to "should" match clause
    fieldBoosts.foreach { case (field, weight) =>
      matchPhrase.field(field.fieldName, weight)
    }

    // Combine the two queries above into a single Boolean query
    val query = QueryBuilders.boolQuery().must(matchTerms).should(matchPhrase)

    // Add datatype boosts (if any). These end up in the should clause.
    // NOTE: These boosts are normalized (i.e., not absolute weights on final scores).
    Boosts.applyDatatypeBoosts(query, datatypeBoosts)

    query
  }

  // NOTE: Advanced queries respect fieldBoosts but not datatypeBoosts
  // Q: Is this expected and desired?
  def generateAdvancedQuery(
      queryString: String,
      fieldBoosts: Map[CeteraFieldType with Boostable, Float])
    : BaseQueryBuilder = {

    val query = QueryBuilders
      .queryStringQuery(queryString)
      .field("fts_analyzed")
      .field("fts_raw")
      .field("domain_cname")
      .autoGeneratePhraseQueries(true)

      fieldBoosts.foreach { case (field, weight) =>
        query.field(field.fieldName, weight)
      }

      query
  }

  private def buildFilteredQuery(
      datatypes: Option[Seq[String]],
      domains: Set[String],
      searchContext: Option[Domain],
      categories: Option[Set[String]],
      tags: Option[Set[String]],
      domainMetadata: Option[Set[(String, String)]],
      query: BaseQueryBuilder)
    : BaseQueryBuilder = {

    import com.socrata.cetera.search.Filters._ // scalastyle:ignore import.grouping

    // If there is no search context, use the ODN categories and tags and prohibit domain metadata
    // otherwise use the custom domain categories, tags, metadata
    val categoriesAndTags: Seq[QueryBuilder] =
      if (searchContext.isDefined) {
        List.concat(
          domainCategoriesQuery(categories),
          domainTagsQuery(tags))
      } else {
        List.concat(
          categoriesQuery(categories),
          tagsQuery(tags))
      }

    val moderated = searchContext.exists(_.moderationEnabled)

    val filters: List[FilterBuilder] = List.concat(
      datatypeFilter(datatypes),
      domainFilter(domains),
      moderationStatusFilter(moderated),
      routingApprovalFilter(searchContext),
      if (searchContext.isDefined) domainMetadataFilter(domainMetadata) else customerDomainFilter
    )

    val categoriesAndTagsQuery =
      if (categoriesAndTags.nonEmpty) {
        categoriesAndTags.foldLeft(QueryBuilders.boolQuery().must(query)) { (b, q) => b.must(q) }
      } else { query }

    if (filters.nonEmpty) {
      QueryBuilders.filteredQuery(
        categoriesAndTagsQuery,
        FilterBuilders.andFilter(filters.toSeq: _*)
      )
    } else { categoriesAndTagsQuery }
  }

  private def applyDefaultTitleBoost(
      fieldBoosts: Map[CeteraFieldType with Boostable, Float])
    : Map[CeteraFieldType with Boostable, Float] = {

    // Look for default title boost; if a title boost is specified as a query
    // parameter, it will override the default
    defaultTitleBoost
      .map(boost => Map(TitleFieldType -> boost) ++ fieldBoosts)
      .getOrElse(fieldBoosts)
  }

  def chooseMinShouldMatch(
      minShouldMatch: Option[String],
      searchContext: Option[Domain])
    : Option[String] = {

    (minShouldMatch, searchContext) match {
      // If a minShouldMatch value is passed in, we must honor that.
      case (Some(msm), _) => minShouldMatch

      // If a minShouldMatch value is absent but a searchContext is present,
      // use default minShouldMatch settings for better precision.
      case (None, Some(sc)) => defaultMinShouldMatch

      // If neither is present, then do not use minShouldMatch.
      case (None, None) => None
    }
  }

  // Assumes validation has already been done
  //
  // Called by buildSearchRequest and buildCountRequest
  //
  // * Chooses query type to be used and constructs query with applicable boosts
  // * Applies function scores (typically views and score) with applicable domain boosts
  // * Applies filters (facets and searchContext-sensitive federation preferences)
  def buildBaseRequest( // scalastyle:ignore parameter.number
      searchQuery: QueryType,
      domains: Set[String],
      searchContext: Option[Domain],
      categories: Option[Set[String]],
      tags: Option[Set[String]],
      domainMetadata: Option[Set[(String, String)]],
      only: Option[Seq[String]],
      fieldBoosts: Map[CeteraFieldType with Boostable, Float],
      datatypeBoosts: Map[Datatype, Float],
      domainBoosts: Map[String, Float],
      minShouldMatch: Option[String],
      slop: Option[Int])
    : SearchRequestBuilder = {

    // Construct basic match query
    val matchQuery = searchQuery match {
      case NoQuery => QueryBuilders.matchAllQuery
      case AdvancedQuery(queryString) => generateAdvancedQuery(queryString, fieldBoosts)
      case SimpleQuery(queryString) => generateSimpleQuery(queryString,
        applyDefaultTitleBoost(fieldBoosts),
        datatypeBoosts,
        chooseMinShouldMatch(minShouldMatch, searchContext),
        slop)
    }

    // Wrap basic match query in filtered query for filtering
    val filteredQuery = buildFilteredQuery(only,
      domains,
      searchContext,
      categories,
      tags,
      domainMetadata,
      matchQuery)

    // Wrap filtered query in function score query for boosting
    val query = QueryBuilders.functionScoreQuery(filteredQuery)
    Boosts.applyScoreFunctions(query, scriptScoreFunctions)
    Boosts.applyDomainBoosts(query, domainBoosts)
    query.scoreMode("multiply").boostMode("replace")

    val preparedSearch = esClient.client
      .prepareSearch(Indices: _*)
      .setQuery(query)
      .setTypes(esDocumentType)

    preparedSearch
  }

  def buildSearchRequest( // scalastyle:ignore parameter.number
      searchQuery: QueryType,
      domains: Set[String],
      domainMetadata: Option[Set[(String, String)]],
      searchContext: Option[Domain],
      categories: Option[Set[String]],
      tags: Option[Set[String]],
      only: Option[Seq[String]],
      fieldBoosts: Map[CeteraFieldType with Boostable, Float],
      datatypeBoosts: Map[Datatype, Float],
      domainBoosts: Map[String, Float],
      minShouldMatch: Option[String],
      slop: Option[Int],
      offset: Int,
      limit: Int)
    : SearchRequestBuilder = {

    val baseRequest = buildBaseRequest(
      searchQuery,
      domains,
      searchContext,
      categories,
      tags,
      domainMetadata,
      only,
      fieldBoosts,
      datatypeBoosts,
      domainBoosts,
      minShouldMatch,
      slop
    )

    // WARN: Sort will totally blow away score if score isn't part of the sort
    val sort = Sorts.chooseSort(searchQuery, searchContext, categories, tags)

    baseRequest
      .setFrom(offset)
      .setSize(limit)
      .addSort(sort)
  }

  def buildCountRequest(
      field: CeteraFieldType with Countable with Rawable,
      searchQuery: QueryType,
      domains: Set[String],
      searchContext: Option[Domain],
      categories: Option[Set[String]],
      tags: Option[Set[String]],
      only: Option[Seq[String]])
    : SearchRequestBuilder = {

    val aggregation = Aggregations.chooseAggregation(field)

    val baseRequest = buildBaseRequest(
      searchQuery,
      domains,
      searchContext,
      categories,
      tags,
      None,
      only,
      Map.empty,
      Map.empty,
      Map.empty,
      None,
      None
    )

    baseRequest
      .addAggregation(aggregation)
      .setSearchType("count")
  }

  def buildFacetRequest(cname: String): SearchRequestBuilder = {
    val size = 0 // no docs, aggs only

    val datatypeAgg = AggregationBuilders
      .terms("datatypes")
      .field(DatatypeFieldType.fieldName)
      .size(size)

    val categoryAgg = AggregationBuilders
      .terms("categories")
      .field(DomainCategoryFieldType.rawFieldName)
      .size(size)

    val tagAgg = AggregationBuilders
      .terms("tags")
      .field(DomainTagsFieldType.rawFieldName)
      .size(size)

    val metadataAgg = AggregationBuilders
      .nested("metadata")
      .path(DomainMetadataFieldType.fieldName)
      .subAggregation(AggregationBuilders.terms("keys")
        .field(DomainMetadataFieldType.Key.rawFieldName)
        .size(size)
        .subAggregation(AggregationBuilders.terms("values")
          .field(DomainMetadataFieldType.Value.rawFieldName)
          .size(size)))

    val filter = Filters.domainFilter(cname)
      .getOrElse(FilterBuilders.matchAllFilter())

    val filteredAggs = AggregationBuilders
      .filter("domain_filter")
      .filter(filter)
      .subAggregation(datatypeAgg)
      .subAggregation(categoryAgg)
      .subAggregation(tagAgg)
      .subAggregation(metadataAgg)

    val preparedSearch = esClient.client
      .prepareSearch(Indices: _*)
      .addAggregation(filteredAggs)
      .setSize(size)

    preparedSearch
  }
}
