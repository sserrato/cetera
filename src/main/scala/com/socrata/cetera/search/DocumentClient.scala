package com.socrata.cetera.search

import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.index.query._
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.sort.{FieldSortBuilder, SortBuilder, SortBuilders, SortOrder}
import org.slf4j.LoggerFactory

import com.socrata.cetera._
import com.socrata.cetera.search.Aggregations._
import com.socrata.cetera.search.Filters._
import com.socrata.cetera.search.SundryBuilders._
import com.socrata.cetera.types._

object DocumentClient {
  def apply(
      esClient: ElasticSearchClient,
      defaultTypeBoosts: Map[String, Float],
      defaultTitleBoost: Option[Float],
      defaultMinShouldMatch: Option[String],
      scriptScoreFunctions: Set[ScriptScoreFunction])
    : DocumentClient = {

    val datatypeBoosts = defaultTypeBoosts.flatMap { case (k, v) =>
      Datatype(k).map(datatype => (datatype, v))
    }

    new DocumentClient(
      esClient,
      datatypeBoosts,
      defaultTitleBoost,
      defaultMinShouldMatch,
      scriptScoreFunctions
    )
  }
}

class DocumentClient(
    esClient: ElasticSearchClient,
    defaultTypeBoosts: Map[Datatype, Float],
    defaultTitleBoost: Option[Float],
    defaultMinShouldMatch: Option[String],
    scriptScoreFunctions: Set[ScriptScoreFunction]) {

  val logger = LoggerFactory.getLogger(getClass)

  private def logESRequest(search: SearchRequestBuilder): Unit =
    logger.info(
      s"""Elasticsearch request
         | indices: ${Indices.mkString(",")},
         | body: ${search.toString.replaceAll("""[\n\s]+""", " ")}
       """.stripMargin
    )

  // This query is complex, as it generates two queries that are then combined
  // into a single query. By default, the must match clause enforces a term match
  // such that one or more of the query terms must be present in at least one of the
  // fields specified. The optional minimum_should_match constraint applies to this
  // clause. The should clause handles applies to a phrase search, with an optional slop
  // param. (specified by the query parameter). This has the effect of giving a boost
  // to documents where there is a phrasal match. The scores are then averaged together
  // by ES with a defacto score of 0 for should a clause if it does not in fact match any
  // documents. See the ElasticSearch documentation here:
  //
  //   https://www.elastic.co/guide/en/elasticsearch/guide/current/proximity-relevance.html
  def generateQuery(
      searchQuery: QueryType,
      fieldBoosts: Map[CeteraFieldType with Boostable, Float],
      typeBoosts: Map[Datatype, Float],
      minShouldMatch: Option[String],
      slop: Option[Int])
    : BaseQueryBuilder = {

    searchQuery match {
      case NoQuery =>
        QueryBuilders.matchAllQuery

      case SimpleQuery(sq) =>
        val matchTerms = multiMatch(sq, MultiMatchQueryBuilder.Type.CROSS_FIELDS)

        // Side effects!
        fieldBoosts.foreach { case (field, weight) => matchTerms.field(field.fieldName, weight) }
        minShouldMatch.foreach(addMinMatchConstraint(matchTerms, _))

        val matchPhrase = multiMatch(sq, MultiMatchQueryBuilder.Type.PHRASE)

        // NOTE: if no slop is specified, we do not set a default
        slop.foreach(addSlopParam(matchPhrase, _))

        // Combines the two queries above into a single Boolean query
        val query = QueryBuilders.boolQuery().must(matchTerms).should(matchPhrase)

        // If we have typeBoosts, add them as should match clause
        if (typeBoosts.nonEmpty) {
          query.should(boostTypes(typeBoosts))
        } else {
          query
        }

      case AdvancedQuery(aq) =>
        val query = QueryBuilders
          .queryStringQuery(aq)
          .field("fts_analyzed")
          .field("fts_raw")
          .field("domain_cname")
          .autoGeneratePhraseQueries(true)

        // Side effects!
        fieldBoosts.foreach { case (field, weight) =>
          query.field(field.fieldName, weight)
        }

        query
    }
  }

  private def buildFilteredQuery(
      datatypes: Option[Seq[String]],
      domains: Option[Set[String]],
      searchContext: Option[Domain],
      categories: Option[Set[String]],
      tags: Option[Set[String]],
      domainMetadata: Option[Set[(String, String)]],
      query: BaseQueryBuilder)
    : BaseQueryBuilder = {

    // If there is no search context, use the ODN categories and tags and prohibit domain metadata
    // otherwise use the custom domain categories, tags, metadata
    val odnFilters = List.concat(
      customerDomainFilter,
      categoriesFilter(categories),
      tagsFilter(tags)
    )

    val domainFilters = List.concat(
      domainCategoriesFilter(categories),
      domainTagsFilter(tags),
      domainMetadataFilter(domainMetadata)
    )

    val moderated = searchContext.exists(_.moderationEnabled)

    val filters = List.concat(
      datatypeFilter(datatypes),
      domainFilter(domains),
      moderationStatusFilter(moderated),
      routingApprovalFilter(searchContext),
      if (searchContext.isDefined) domainFilters else odnFilters
    )

    if (filters.nonEmpty) {
      QueryBuilders.filteredQuery(
        query,
        FilterBuilders.andFilter(filters.toSeq: _*)
      )
    } else {
      query
    }
  }

  // Assumes validation has already been done
  def buildBaseRequest( // scalastyle:ignore parameter.number
      searchQuery: QueryType,
      domains: Option[Set[String]],
      searchContext: Option[Domain],
      categories: Option[Set[String]],
      tags: Option[Set[String]],
      domainMetadata: Option[Set[(String, String)]],
      only: Option[Seq[String]],
      fieldBoosts: Map[CeteraFieldType with Boostable, Float],
      datatypeBoosts: Map[Datatype, Float],
      minShouldMatch: Option[String],
      slop: Option[Int])
    : SearchRequestBuilder = {

    val matchQuery: BaseQueryBuilder = generateQuery(
      searchQuery,

      // Look for default title boost; if a title boost is specified as a query
      // parameter, it will override the default
      defaultTitleBoost.map(boost => Map(TitleFieldType -> boost) ++ fieldBoosts).getOrElse(fieldBoosts),
      defaultTypeBoosts ++ datatypeBoosts,

      // If we're doing a within-domain catalog search then we want to optimize
      // for precision so by default, we use the defaultMinShouldMatch setting;
      // but we'll always honor the parameter value passed in with the query
      minShouldMatch.orElse(if (searchContext.isDefined) defaultMinShouldMatch else None),
      slop
    )

    val query = if (scriptScoreFunctions.nonEmpty) {
      addFunctionScores(
        scriptScoreFunctions,
        QueryBuilders.functionScoreQuery(matchQuery)
      )
    } else {
      matchQuery
    }

    val filteredQuery = buildFilteredQuery(
      only,
      domains,
      searchContext,
      categories,
      tags,
      domainMetadata,
      query
    )

    // Imperative builder --> order is important
    val preparedSearch = esClient.client
      .prepareSearch(Indices: _*)
      .setQuery(filteredQuery)
      .setTypes(esDocumentType)

    logESRequest(preparedSearch)
    preparedSearch
  }

  def buildAverageScoreSort(
      fieldName: String,
      rawFieldName: String,
      classifications: Set[String])
    : FieldSortBuilder = {

    SortBuilders
      .fieldSort(fieldName)
      .order(SortOrder.DESC)
      .sortMode("avg")
      .setNestedFilter(
        FilterBuilders.termsFilter(
          rawFieldName,
          classifications.toSeq: _*
        )
      )
  }

  // TODO: Possibly chooseSort is a better name
  // First pass logic is very simple. query >> categories >> tags >> default
  def buildSort(
      searchQuery: QueryType,
      searchContext: Option[Domain],
      categories: Option[Set[String]],
      tags: Option[Set[String]])
    : SortBuilder = {

    (searchQuery, categories, tags) match {
      // Query
      case (AdvancedQuery(_) | SimpleQuery(_), _, _) => sortScoreDesc

      // ODN Categories
      // Q: What happens when we search according to domain_categories? It's not clear
      case (_, Some(cats), _) if searchContext.isEmpty =>
        buildAverageScoreSort(
          CategoriesFieldType.Score.fieldName,
          CategoriesFieldType.Name.rawFieldName,
          cats
        )

      // ODN Tags
      case (_, _, Some(ts)) if searchContext.isEmpty =>
        buildAverageScoreSort(
          TagsFieldType.Score.fieldName,
          TagsFieldType.Name.rawFieldName,
          ts
        )

      // Default (No query, categories, or tags)
      case (_, _, _) =>
        sortFieldDesc(PageViewsTotalFieldType.fieldName)
    }
  }

  def buildSearchRequest( // scalastyle:ignore parameter.number
      searchQuery: QueryType,
      domains: Option[Set[String]],
      domainMetadata: Option[Set[(String, String)]],
      searchContext: Option[Domain],
      categories: Option[Set[String]],
      tags: Option[Set[String]],
      only: Option[Seq[String]],
      fieldBoosts: Map[CeteraFieldType with Boostable, Float],
      datatypeBoosts: Map[Datatype, Float],
      minShouldMatch: Option[String],
      slop: Option[Int],
      offset: Int,
      limit: Int,
      advancedQuery: Option[String] = None)
    : SearchRequestBuilder = {

    val sort = buildSort(searchQuery, searchContext, categories, tags)

    buildBaseRequest(searchQuery, domains, searchContext, categories, tags, domainMetadata,
                     only, fieldBoosts, datatypeBoosts, minShouldMatch, slop)
      .setFrom(offset)
      .setSize(limit)
      .addSort(sort)
  }

  def buildCountRequest(
      field: CeteraFieldType with Countable with Rawable,
      searchQuery: QueryType,
      domains: Option[Set[String]],
      searchContext: Option[Domain],
      categories: Option[Set[String]],
      tags: Option[Set[String]],
      only: Option[Seq[String]])
    : SearchRequestBuilder = {

    val aggregation = field match {
      case DomainFieldType => aggDomain
      case CategoriesFieldType => aggCategories
      case TagsFieldType => aggTags
      case DomainCategoryFieldType => aggDomainCategory
    }

    buildBaseRequest(searchQuery, domains, searchContext, categories, tags,
                     None, only, Map.empty, Map.empty, None, None)
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

    val filter = Option(cname) match {
      case Some(s) if s.nonEmpty => domainFilter(Some(Set(cname))).getOrElse(throw new NoSuchElementException)
      case _ => FilterBuilders.matchAllFilter()
    }

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

    logESRequest(preparedSearch)
    preparedSearch
  }
}

