package com.socrata.cetera.search

import java.io.Closeable

import com.socrata.cetera._
import com.socrata.cetera.types._
import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.index.query.functionscore.{FunctionScoreQueryBuilder, ScoreFunctionBuilders}
import org.elasticsearch.index.query.{BaseQueryBuilder, FilterBuilders, MultiMatchQueryBuilder, QueryBuilders}
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.elasticsearch.search.sort.{SortBuilder, SortBuilders, SortOrder}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

class ElasticSearchClient(host: String,
                          port: Int,
                          clusterName: String,
                          useCustomRanker: Boolean = false) extends Closeable {
  val log = LoggerFactory.getLogger(getClass)

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

  private def addFunctionScores(query: FunctionScoreQueryBuilder,
                        functions: List[ScriptScoreFunction]) = {
    functions.foreach { fn =>
      query.add(ScoreFunctionBuilders.scriptFunction(fn.script, "expression", fn.params.asJava))
    }

    query.scoreMode("sum")
      .boostMode("replace")
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
  def generateQuery(searchQuery: QueryType,
                    boosts: Map[CeteraFieldType with Boostable,Float],
                    minShouldMatch: Option[String],
                    slop: Option[Int]): BaseQueryBuilder = {
    searchQuery match {
      case NoQuery => QueryBuilders.matchAllQuery
      case SimpleQuery(sq) =>
        val matchTerms = multiMatch(sq, MultiMatchQueryBuilder.Type.CROSS_FIELDS)

        // Side effects!
        boosts.foreach { case (field, weight) => matchTerms.field(field.fieldName, weight) }

        minShouldMatch.foreach(addMinMatchConstraint(matchTerms, _))

        val matchPhrase = multiMatch(sq, MultiMatchQueryBuilder.Type.PHRASE)

        // Note, if no slop is specified, we do not set a default
        slop.foreach(addSlopParam(matchPhrase, _))

        // Combines the two queries above into a single Boolean query
        QueryBuilders.boolQuery().must(matchTerms).should(matchPhrase)

      case AdvancedQuery(aq) =>
        val query = QueryBuilders.queryStringQuery(aq).
          field("fts_analyzed").
          field("fts_raw").
          field("domain_cname").
          autoGeneratePhraseQueries(true)

        // Side effects!
        boosts.foreach {
          case (field, weight) => query.field(field.fieldName, weight)
        }

        query
    }
  }

  private def domainFilter(domains: Option[Set[String]]) =
    domains.map { ds => FilterBuilders.termsFilter(DomainFieldType.rawFieldName, ds.toSeq:_*) }

  private def categoriesFilter(categories: Option[Set[String]], searchContext: Option[String]) = categories.map { cs =>
    // If there is no search context, use the ODN categories, otherwise use the customer categories
    if (searchContext.isDefined) {
      FilterBuilders.termsFilter(CustomerCategoryFieldType.rawFieldName, cs.toSeq: _*)
    } else {
      FilterBuilders.nestedFilter(
        CategoriesFieldType.fieldName,
        FilterBuilders.termsFilter(CategoriesFieldType.rawFieldName, cs.toSeq: _*))
    }
  }

  private def tagsFilter(tags: Option[Set[String]]) = tags.map { tags =>
    FilterBuilders.nestedFilter(TagsFieldType.fieldName,
      FilterBuilders.termsFilter(TagsFieldType.rawFieldName, tags.toSeq:_*))
  }

  // Assumes validation has already been done
  def buildBaseRequest(searchQuery: QueryType, // scalastyle:ignore parameter.number
                       domains: Option[Set[String]],
                       searchContext: Option[String],
                       categories: Option[Set[String]],
                       tags: Option[Set[String]],
                       only: Option[String],
                       boosts: Map[CeteraFieldType with Boostable, Float],
                       minShouldMatch: Option[String],
                       slop: Option[Int],
                       functionScores: List[ScriptScoreFunction]): SearchRequestBuilder = {
    // use an if for the NoQuery and factor everything else out
    // OR leave this as is because we're working
    val matchQuery: BaseQueryBuilder = generateQuery(searchQuery, boosts, minShouldMatch, slop)

    val q = if (functionScores.nonEmpty) {
      val query = QueryBuilders.functionScoreQuery(matchQuery)
      addFunctionScores(query, functionScores)
    } else { matchQuery }

    val query = locally {
      val filters = List.concat(domainFilter(domains), categoriesFilter(categories, searchContext), tagsFilter(tags))
      if (filters.nonEmpty) { QueryBuilders.filteredQuery(q, FilterBuilders.andFilter(filters:_*)) } else { q }
    }

    // Imperative builder --> order is important
    val finalQuery = client
      .prepareSearch(Indices: _*)
      .setTypes(only.toList: _*)

    if (useCustomRanker) {
      val custom = QueryBuilders.functionScoreQuery(query).boostMode("replace")
      val script = ScoreFunctionBuilders.scriptFunction(
        "cetera-ranker",
        "native",
        Map("boostLastUpdatedAtValue"-> 1.5,
            "boostPopularityValue" -> 1.0).asInstanceOf[Map[String,Object]].asJava
      )
      custom.add(script)
      log.info(custom.toString)
      finalQuery.setQuery(custom)
    } else { finalQuery.setQuery(query) }
  }

  private val sortScoreDesc: SortBuilder = SortBuilders.scoreSort().order(SortOrder.DESC)
  private def sortFieldAsc(field: String): SortBuilder = SortBuilders.fieldSort(field).order(SortOrder.ASC)

  // First pass logic is very simple. advanced query >> query >> categories >> tags
  def buildSearchRequest(searchQuery: QueryType, // scalastyle:ignore parameter.number
                         domains: Option[Set[String]],
                         searchContext: Option[String],
                         categories: Option[Set[String]],
                         tags: Option[Set[String]],
                         only: Option[String],
                         boosts: Map[CeteraFieldType with Boostable, Float],
                         minShouldMatch: Option[String],
                         slop: Option[Int],
                         functionScores: List[ScriptScoreFunction],
                         offset: Int,
                         limit: Int,
                         advancedQuery: Option[String] = None ): SearchRequestBuilder = {
    val sort = (searchQuery, categories, tags) match {
      case (NoQuery, None, None) => sortFieldAsc(TitleFieldType.rawFieldName)
      case (AdvancedQuery(_) | SimpleQuery(_), _, _) => sortScoreDesc // Query

      // Categories
      case (_, Some(cats), _) if searchContext.isEmpty =>
        SortBuilders
          .fieldSort("animl_annotations.categories.score")
          .order(SortOrder.DESC)
          .sortMode("avg")
          .setNestedFilter(FilterBuilders.termsFilter(CategoriesFieldType.rawFieldName, cats.toSeq:_*))

      // Categories and search context
      // TODO: Should we sort by popularity?
      case (_, Some(cats), _) if searchContext.isDefined => sortFieldAsc(TitleFieldType.rawFieldName)

      // Tags
      case (_, _, Some(ts)) if searchContext.isEmpty =>
        SortBuilders
          .fieldSort("animl_annotations.tags.score")
          .order(SortOrder.DESC)
          .sortMode("avg")
          .setNestedFilter(FilterBuilders.termsFilter(TagsFieldType.rawFieldName, ts.toSeq:_*))

      case (_ , _, Some(ts)) => sortFieldAsc(TitleFieldType.rawFieldName)
    }

    buildBaseRequest(searchQuery, domains, searchContext, categories, tags,
      only, boosts, minShouldMatch, slop, functionScores)
      .setFrom(offset)
      .setSize(limit)
      .addSort(sort)
  }

  private def aggDomain(field: CeteraFieldType with Countable with Rawable) =
    AggregationBuilders
      .terms("domains")
      .field(field.rawFieldName)
      .order(Terms.Order.count(false)) // count desc
      .size(0) // no docs, aggs only

  private def aggCategories(field: CeteraFieldType with Countable with Rawable) =
    AggregationBuilders
      .nested("annotations")
      .path(field.fieldName)
      .subAggregation(
        AggregationBuilders
          .terms("names")
          .field(field.rawFieldName)
          .size(0)
      )

  private def aggTags(field: CeteraFieldType with Countable with Rawable) =
    AggregationBuilders
      .nested("annotations")
      .path(field.fieldName)
      .subAggregation(
        AggregationBuilders
          .terms("names")
          .field(field.rawFieldName)
          .size(0)
      )

  private def aggCustomerCategory(field: CeteraFieldType with Countable with Rawable) =
    AggregationBuilders
      .terms("categories")
      .field(field.rawFieldName)
      .order(Terms.Order.count(false)) // count desc
      .size(0) // no docs, aggs only

  def buildCountRequest(field: CeteraFieldType with Countable with Rawable,
                        searchQuery: QueryType,
                        domains: Option[Set[String]],
                        searchContext: Option[String],
                        categories: Option[Set[String]],
                        tags: Option[Set[String]],
                        only: Option[String]): SearchRequestBuilder = {
    val aggregation = field match {
      case DomainFieldType => aggDomain(field)
      case CategoriesFieldType => aggCategories(field)
      case TagsFieldType => aggTags(field)
      case CustomerCategoryFieldType => aggCustomerCategory(field)
    }

    buildBaseRequest(searchQuery, domains, searchContext, categories, tags, only, Map.empty, None, None, List.empty)
      .addAggregation(aggregation)
      .setSearchType("count")
  }

  def buildFacetRequest(cname: String): SearchRequestBuilder = {
    val size = 0 // no docs, aggs only

    val query = QueryBuilders.matchQuery("domain_cname", cname)
    val categoryAgg = AggregationBuilders.terms("categories")
      .field(CustomerCategoryFieldType.fieldName + ".raw")
      .size(size)
    val tagAgg = AggregationBuilders.terms("tags")
      .field(CustomerTagsFieldType.fieldName + ".raw")
      .size(size)
    val metadataAgg = AggregationBuilders.nested("metadata")
      .path(CustomerMetadataFlattenedPartialFieldType.fieldName)
      .subAggregation(AggregationBuilders.terms("kvp")
        .field(CustomerMetadataFlattenedPartialFieldType.fieldName + ".key.raw")
        .size(size))
    client.prepareSearch(Indices: _*)
      .setQuery(query)
      .addAggregation(categoryAgg)
      .addAggregation(tagAgg)
      .addAggregation(metadataAgg)
      .setSize(size)
  }

  def buildFacetValueRequest(cname: String, facet: Option[String] = None): SearchRequestBuilder = {
    val size = 0 // no docs, aggs only

    val query = QueryBuilders.boolQuery()
      .must(QueryBuilders.matchQuery("domain_cname", cname))
      .must(facet match {
        case None => QueryBuilders.matchAllQuery()
        case Some(f) => QueryBuilders.nestedQuery(
          CustomerMetadataFlattenedPartialFieldType.fieldName,
          QueryBuilders.matchQuery("key", f))
      })
    val metadataAgg = AggregationBuilders.nested("metadata")
      .path(CustomerMetadataFlattenedPartialFieldType.fieldName)
      .subAggregation(AggregationBuilders.terms("kvp")
        .field(CustomerMetadataFlattenedPartialFieldType.fieldName + ".value.raw")
        .size(size)
      )
    client.prepareSearch(Indices: _*)
      .setQuery(query)
      .addAggregation(metadataAgg)
      .setSize(size)
  }
}
