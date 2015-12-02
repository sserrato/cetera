package com.socrata.cetera.search

import java.io.Closeable

import com.socrata.cetera._
import com.socrata.cetera.types._
import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.index.query._
import org.elasticsearch.index.query.functionscore.{FunctionScoreQueryBuilder, ScoreFunctionBuilders}
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.elasticsearch.search.sort.{SortBuilder, SortBuilders, SortOrder}
import org.slf4j.LoggerFactory

object ElasticSearchClient {
  def apply(
    host: String,
    port: Int,
    clusterName: String,
    defaultTypeBoosts: Map[String, Float],
    defaultTitleBoost: Option[Float],
    defaultMinShouldMatch: Option[String],
    scriptScoreFunctions: Set[ScriptScoreFunction]
  ): ElasticSearchClient = {
    val datatypeBoosts = defaultTypeBoosts.flatMap { case (k, v) =>
      DatatypeSimple(k).map(datatype => (datatype, v))
    }

    new ElasticSearchClient(
      host, port, clusterName, datatypeBoosts, defaultTitleBoost,
      defaultMinShouldMatch, scriptScoreFunctions)
  }
}


class ElasticSearchClient(
  host: String,
  port: Int,
  clusterName: String,
  defaultTypeBoosts: Map[DatatypeSimple, Float],
  defaultTitleBoost: Option[Float],
  defaultMinShouldMatch: Option[String],
  scriptScoreFunctions: Set[ScriptScoreFunction]
) extends Closeable {
  val logger = LoggerFactory.getLogger(getClass)

  val settings = ImmutableSettings.settingsBuilder()
    .put("cluster.name", clusterName)
    .put("client.transport.sniff", true)
    .build()

  val client: Client = new TransportClient(settings)
    .addTransportAddress(new InetSocketTransportAddress(host, port))

  def close(): Unit = client.close()

  private def addMinMatchConstraint(query: MultiMatchQueryBuilder, constraint: String) =
    query.minimumShouldMatch(constraint)

  private def addSlopParam(query: MultiMatchQueryBuilder, slop: Int) = query.slop(slop)

  private def addFunctionScores(query: FunctionScoreQueryBuilder) = {
    scriptScoreFunctions.foreach { fn =>
      query.add(ScoreFunctionBuilders.scriptFunction(fn.script, "expression"))
    }

    // Take a product of scores and replace original score with product
    query.scoreMode("multiply").boostMode("replace")
  }

  private def boostTypes(typeBoosts: Map[DatatypeSimple, Float]) = {
    typeBoosts.foldLeft(QueryBuilders.boolQuery()) { case (q, (datatype, boost)) =>
      q.should(QueryBuilders.termQuery(DatatypeFieldType.fieldName, datatype.singular).boost(boost))
    }
  }

  private def multiMatch(q: String, mmType: MultiMatchQueryBuilder.Type) = QueryBuilders.multiMatchQuery(q).
    field("fts_analyzed").
    field("fts_raw").
    field("domain_cname").
    `type`(mmType)

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
    typeBoosts: Map[DatatypeSimple, Float],
    minShouldMatch: Option[String],
    slop: Option[Int]
  ): BaseQueryBuilder = {
    searchQuery match {
      case NoQuery => QueryBuilders.matchAllQuery

      case SimpleQuery(sq) =>
        val matchTerms = multiMatch(sq, MultiMatchQueryBuilder.Type.CROSS_FIELDS)

        // Side effects!
        fieldBoosts.foreach { case (field, weight) => matchTerms.field(field.fieldName, weight) }

        minShouldMatch.foreach(addMinMatchConstraint(matchTerms, _))

        val matchPhrase = multiMatch(sq, MultiMatchQueryBuilder.Type.PHRASE)

        // Note, if no slop is specified, we do not set a default
        slop.foreach(addSlopParam(matchPhrase, _))

        // Combines the two queries above into a single Boolean query
        val q = QueryBuilders.boolQuery().must(matchTerms).should(matchPhrase)

        // If we have typeBoosts, add them as should match clause
        if (typeBoosts.nonEmpty) {
          q.should(boostTypes(typeBoosts))
        } else {
          q
        }

      case AdvancedQuery(aq) =>
        val query = QueryBuilders.queryStringQuery(aq).
          field("fts_analyzed").
          field("fts_raw").
          field("domain_cname").
          autoGeneratePhraseQueries(true)

        // Side effects!
        fieldBoosts.foreach {
          case (field, weight) => query.field(field.fieldName, weight)
        }

        query
    }
  }

  private def datatypeFilter(datatypes: Option[Seq[String]]) =
    datatypes.map { ts =>
      val validatedDatatypes = ts.map(t => DatatypeSimple(t).map(_.singular)).flatten
      FilterBuilders.termsFilter(DatatypeFieldType.fieldName, validatedDatatypes: _*)
    }

  private def domainFilter(domains: Option[Set[String]]) =
    domains.map { ds => FilterBuilders.termsFilter(DomainFieldType.rawFieldName, ds.toSeq: _*) }

  private def categoriesFilter(categories: Option[Set[String]]) = categories.map { cs =>
    FilterBuilders.nestedFilter(
      CategoriesFieldType.fieldName,
      FilterBuilders.termsFilter(CategoriesFieldType.Name.rawFieldName, cs.toSeq: _*)
    )
  }

  private def tagsFilter(tags: Option[Set[String]]) = tags.map { tags =>
    FilterBuilders.nestedFilter(
      TagsFieldType.fieldName,
      FilterBuilders.termsFilter(TagsFieldType.Name.rawFieldName, tags.toSeq: _*)
    )
  }

  private def domainCategoriesFilter(categories: Option[Set[String]]) = categories.map { cs =>
    FilterBuilders.termsFilter(DomainCategoryFieldType.rawFieldName, cs.toSeq: _*)
  }

  private def domainTagsFilter(tags: Option[Set[String]]) = tags.map { ts =>
    FilterBuilders.termsFilter(DomainTagsFieldType.rawFieldName, ts.toSeq: _*)
  }

  private def domainMetadataFilter(metadata: Option[Set[(String, String)]]) = metadata.map { ss =>
    FilterBuilders.orFilter(
      ss.map { kvp =>
        FilterBuilders.nestedFilter(
          DomainMetadataFieldType.fieldName,
          FilterBuilders.andFilter(
            FilterBuilders.termsFilter(DomainMetadataFieldType.Key.rawFieldName, kvp._1),
            FilterBuilders.termsFilter(DomainMetadataFieldType.Value.rawFieldName, kvp._2)
          )
        )
      }.toSeq: _*
    )
  }

  private def customerDomainFilter =
    Some(FilterBuilders.notFilter(FilterBuilders.termsFilter(IsCustomerDomainFieldType.fieldName, "false")))

  private def moderationStatusFilter =
    Some(FilterBuilders.notFilter(
      FilterBuilders.termsFilter(ModerationStatusFieldType.fieldName, "pending", "rejected")
    ))

  // Assumes validation has already been done
  def buildBaseRequest( // scalastyle:ignore parameter.number
    searchQuery: QueryType,
    domains: Option[Set[String]],
    searchContext: Option[String],
    categories: Option[Set[String]],
    tags: Option[Set[String]],
    domainMetadata: Option[Set[(String, String)]],
    only: Option[Seq[String]],
    fieldBoosts: Map[CeteraFieldType with Boostable, Float],
    datatypeBoosts: Map[DatatypeSimple, Float],
    minShouldMatch: Option[String],
    slop: Option[Int]
  ): SearchRequestBuilder = {
    val matchQuery: BaseQueryBuilder = generateQuery(
      searchQuery,
      // Look for default title boost; if a title boost is specified as a query parameter, it
      // will override the default
      defaultTitleBoost.map(boost => Map(TitleFieldType -> boost) ++ fieldBoosts).getOrElse(fieldBoosts),
      defaultTypeBoosts ++ datatypeBoosts,
      // If we're doing a within-domain catalog search then we want to optimize for precision
      // so by default, we use the defaultMinShouldMatch setting; but we'll always honor the parameter
      // value passed in with the query
      minShouldMatch.orElse(if (searchContext.isDefined) defaultMinShouldMatch else None),
      slop
    )

    val q = if (scriptScoreFunctions.nonEmpty) {
      val query = QueryBuilders.functionScoreQuery(matchQuery)
      addFunctionScores(query)
    } else { matchQuery }

    val query: BaseQueryBuilder = selectSearchContext(only, domains, searchContext, categories, tags, domainMetadata, q)

    // Imperative builder --> order is important
    val search = client.prepareSearch(Indices: _*)
      .setQuery(query)
      .setTypes(esDocumentType)
    logESRequest(search)
    search
  }

  private def logESRequest(search: SearchRequestBuilder): Unit =
    logger.info(
      s"""Elasticsearch request
         | indices: ${Indices.mkString(",")},
         | body: ${search.toString.replaceAll("""[\n\s]+""", " ")}
       """.stripMargin
    )

  private def selectSearchContext(
                                 datatypes: Option[Seq[String]],
                                 domains: Option[Set[String]],
                                 searchContext: Option[String],
                                 categories: Option[Set[String]],
                                 tags: Option[Set[String]],
                                 domainMetadata: Option[Set[(String, String)]],
                                 q: BaseQueryBuilder): BaseQueryBuilder = {
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
    val filters = List.concat(
      datatypeFilter(datatypes),
      domainFilter(domains),
      moderationStatusFilter,
      if (searchContext.isDefined) domainFilters else odnFilters
    )
    if (filters.nonEmpty) {
      QueryBuilders.filteredQuery(q, FilterBuilders.andFilter(filters.toSeq: _*))
    } else {
      q
    }
  }

  private val sortScoreDesc: SortBuilder = SortBuilders.scoreSort().order(SortOrder.DESC)
  private def sortFieldAsc(field: String): SortBuilder = SortBuilders.fieldSort(field).order(SortOrder.ASC)
  private def sortFieldDesc(field: String): SortBuilder = SortBuilders.fieldSort(field).order(SortOrder.DESC)

  // First pass logic is very simple. advanced query >> query >> categories >> tags >> default
  def buildSearchRequest( // scalastyle:ignore parameter.number
    searchQuery: QueryType,
    domains: Option[Set[String]],
    domainMetadata: Option[Set[(String, String)]],
    searchContext: Option[String],
    categories: Option[Set[String]],
    tags: Option[Set[String]],
    only: Option[Seq[String]],
    fieldBoosts: Map[CeteraFieldType with Boostable, Float],
    datatypeBoosts: Map[DatatypeSimple, Float],
    minShouldMatch: Option[String],
    slop: Option[Int],
    offset: Int,
    limit: Int,
    advancedQuery: Option[String] = None
  ): SearchRequestBuilder = {
    val sort = (searchQuery, categories, tags) match {
      // Query
      case (AdvancedQuery(_) | SimpleQuery(_), _, _) => sortScoreDesc

      // ODN Categories
      case (_, Some(cats), _) if searchContext.isEmpty =>
        SortBuilders
          .fieldSort(CategoriesFieldType.Score.fieldName)
          .order(SortOrder.DESC)
          .sortMode("avg")
          .setNestedFilter(FilterBuilders.termsFilter(CategoriesFieldType.Name.rawFieldName, cats.toSeq: _*))

      // ODN Tags
      case (_, _, Some(ts)) if searchContext.isEmpty =>
        SortBuilders
          .fieldSort(TagsFieldType.Score.fieldName)
          .order(SortOrder.DESC)
          .sortMode("avg")
          .setNestedFilter(FilterBuilders.termsFilter(TagsFieldType.Name.rawFieldName, ts.toSeq: _*))

      // Default
      case (_, _, _) => sortFieldDesc(PageViewsTotalFieldType.fieldName)
    }

    buildBaseRequest(searchQuery, domains, searchContext, categories, tags, domainMetadata,
                     only, fieldBoosts, datatypeBoosts, minShouldMatch, slop)
      .setFrom(offset)
      .setSize(limit)
      .addSort(sort)
  }

  private def aggDomain =
    AggregationBuilders
      .terms("domains")
      .field(DomainFieldType.rawFieldName)
      .order(Terms.Order.count(false)) // count desc
      .size(0) // no docs, aggs only

  private def aggCategories =
    AggregationBuilders
      .nested("annotations")
      .path(CategoriesFieldType.fieldName)
      .subAggregation(
        AggregationBuilders
          .terms("names")
          .field(CategoriesFieldType.Name.rawFieldName)
          .size(0)
      )

  private def aggTags =
    AggregationBuilders
      .nested("annotations")
      .path(TagsFieldType.fieldName)
      .subAggregation(
        AggregationBuilders
          .terms("names")
          .field(TagsFieldType.Name.rawFieldName)
          .size(0)
      )

  private def aggDomainCategory =
    AggregationBuilders
      .terms("categories")
      .field(DomainCategoryFieldType.rawFieldName)
      .order(Terms.Order.count(false)) // count desc
      .size(0) // no docs, aggs only

  def buildCountRequest(
    field: CeteraFieldType with Countable with Rawable,
    searchQuery: QueryType,
    domains: Option[Set[String]],
    searchContext: Option[String],
    categories: Option[Set[String]],
    tags: Option[Set[String]],
    only: Option[Seq[String]]
  ): SearchRequestBuilder = {
    val aggregation = field match {
      case DomainFieldType => aggDomain
      case CategoriesFieldType => aggCategories
      case TagsFieldType => aggTags
      case DomainCategoryFieldType => aggDomainCategory
    }

    buildBaseRequest(searchQuery, domains, searchContext, categories, tags, None, only,
                     Map.empty, Map.empty, None, None)
      .addAggregation(aggregation)
      .setSearchType("count")
  }

  private def domainQuery(cname: String): QueryBuilder = cname.trim match {
    case s: String if s.nonEmpty => QueryBuilders.matchQuery("domain_cname", s)
    case _ => QueryBuilders.matchAllQuery()
  }

  def buildFacetRequest(cname: String): SearchRequestBuilder = {
    val size = 0 // no docs, aggs only

    val datatypeAgg = AggregationBuilders.terms("datatypes")
      .field(DatatypeFieldType.fieldName)
      .size(size)
    val categoryAgg = AggregationBuilders.terms("categories")
      .field(DomainCategoryFieldType.rawFieldName)
      .size(size)
    val tagAgg = AggregationBuilders.terms("tags")
      .field(DomainTagsFieldType.rawFieldName)
      .size(size)
    val metadataAgg = AggregationBuilders.nested("metadata")
      .path(DomainMetadataFieldType.fieldName)
      .subAggregation(AggregationBuilders.terms("keys")
        .field(DomainMetadataFieldType.Key.rawFieldName)
        .size(size)
        .subAggregation(AggregationBuilders.terms("values")
          .field(DomainMetadataFieldType.Value.rawFieldName)
          .size(size)
      ))

    val filter = Option(cname) match {
      case Some(s) if s.nonEmpty => domainFilter(Some(Set(cname))).getOrElse(throw new NoSuchElementException)
      case _ => FilterBuilders.matchAllFilter()
    }
    val filteredAggs = AggregationBuilders.filter("domain_filter")
      .filter(filter)
      .subAggregation(datatypeAgg)
      .subAggregation(categoryAgg)
      .subAggregation(tagAgg)
      .subAggregation(metadataAgg)

    val search = client.prepareSearch(Indices: _*)
      .addAggregation(filteredAggs)
      .setSize(size)
    logESRequest(search)
    search
  }
}
