package com.socrata.cetera.search

import java.io.Closeable
import scala.collection.JavaConverters._

import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.index.query.{MultiMatchQueryBuilder, BaseQueryBuilder}
import org.elasticsearch.index.query.functionscore.{ScoreFunctionBuilders, FunctionScoreQueryBuilder}
import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.index.query.{FilterBuilders, QueryBuilders}
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.elasticsearch.search.sort.{SortBuilders, SortOrder}

import com.socrata.cetera.search.EnrichedFieldTypesForES._
import com.socrata.cetera.types._

class ElasticSearchClient(host: String, port: Int, clusterName: String, useCustomRanker: Boolean = false) extends Closeable {
  val settings = ImmutableSettings.settingsBuilder()
                                  .put("cluster.name", clusterName)
                                  .put("client.transport.sniff", true)
                                  .build()

  val client: Client = new TransportClient(settings)
    .addTransportAddress(new InetSocketTransportAddress(host, port))

  def close(): Unit = client.close()

  // Assumes validation has already been done
  def buildBaseRequest(searchQuery: QueryType,
                       domains: Option[Set[String]],
                       searchContext: Option[String],
                       categories: Option[Set[String]],
                       tags: Option[Set[String]],
                       only: Option[String],
                       boosts: Map[CeteraFieldType with Boostable, Float],
                       minShouldMatch: Option[String],
                       slop: Option[Int],
                       functionScores: List[ScriptScoreFunction]): SearchRequestBuilder = {

    def addMinMatchConstraint(query: MultiMatchQueryBuilder, constraint: String) = {
      query.minimumShouldMatch(constraint)
    }

    def addSlopParam(query: MultiMatchQueryBuilder, slop: Int) = {
      query.slop(slop)
    }

    def addFunctionScores(query: FunctionScoreQueryBuilder,
                          functions: List[ScriptScoreFunction]) = {
      functions.foreach { fn =>
        query.add(ScoreFunctionBuilders.scriptFunction(fn.script, "expression", fn.params.asJava))
      }

      query.scoreMode("sum")
        .boostMode("replace")
    }

    // use an if for the NoQuery and factor everything else out
    // OR leave this as is because we're working
    val matchQuery: BaseQueryBuilder = searchQuery match {
      case NoQuery => QueryBuilders.matchAllQuery

      case SimpleQuery(sq) =>
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

        val matchTerms = QueryBuilders.multiMatchQuery(sq).
          field("fts_analyzed").
          field("fts_raw").
          field("domain_cname").
          `type`(MultiMatchQueryBuilder.Type.CROSS_FIELDS)

        // Side effects!
        boosts.foreach {
          case (field, weight) =>
            matchTerms.field(field.fieldName, weight)
        }

        minShouldMatch.foreach(addMinMatchConstraint(matchTerms, _))

        val matchPhrase = QueryBuilders.multiMatchQuery(sq).
          field("fts_analyzed").
          field("fts_raw").
          field("domain_cname").
          `type`(MultiMatchQueryBuilder.Type.PHRASE)

        // Note, if no slop is specified, we do not set a default
        slop.foreach(addSlopParam(matchPhrase, _))

        // Combines the two queries above into a single Boolean query
        QueryBuilders.boolQuery().
          must(matchTerms).
          should(matchPhrase)

      case AdvancedQuery(aq) =>
        val query = QueryBuilders.queryString(aq).
          field("fts_analyzed").
          field("fts_raw").
          field("domain_cname").
          autoGeneratePhraseQueries(true)

        // Side effects!
        boosts.foreach {
          case (field, weight) =>
            query.field(field.fieldName, weight)
        }

        query
    }

    val q = if (functionScores.nonEmpty) {
      val query = QueryBuilders.functionScoreQuery(matchQuery)
      addFunctionScores(query, functionScores)
    } else matchQuery

    val query = locally {
      val domainFilter = domains.map { domains =>
        FilterBuilders.termsFilter(DomainFieldType.rawFieldName, domains.toSeq:_*)
      }

      val categoriesFilter = categories.map { categories =>
        // If there is no search context, use the ODN categories, otherwise use the customer categories
        if (searchContext.isDefined)
          FilterBuilders.termsFilter(CustomerCategoryFieldType.rawFieldName, categories.toSeq: _*)
        else
          FilterBuilders.nestedFilter(
            CategoriesFieldType.fieldName,
            FilterBuilders.termsFilter(CategoriesFieldType.rawFieldName, categories.toSeq: _*))
      }

      val tagsFilter = tags.map { tags =>
        FilterBuilders.nestedFilter(TagsFieldType.fieldName,
          FilterBuilders.termsFilter(TagsFieldType.rawFieldName, tags.toSeq:_*))
      }

      val filters = List.concat(domainFilter, categoriesFilter, tagsFilter)

      if (filters.nonEmpty) {
        QueryBuilders.filteredQuery(q, FilterBuilders.andFilter(filters:_*))
      } else {
        q
      }
    }

    // Imperative builder --> order is important
    val finalQuery = client
      .prepareSearch("datasets", "files", "hrefs", "maps") // literals should not be here
      .setTypes(only.toList:_*)

    if (useCustomRanker) {
      val custom = QueryBuilders.functionScoreQuery(query).boostMode("replace")
      val script = ScoreFunctionBuilders.scriptFunction(
        "cetera-ranker",
        "native",
        Map("boostLastUpdatedAtValue"-> 1.5,
            "boostPopularityValue" -> 1.0).asInstanceOf[Map[String,Object]].asJava
      )
      custom.add(script)
      println(custom.toString)
      finalQuery.setQuery(custom)
    }
    else finalQuery.setQuery(query)
  }

  def buildSearchRequest(searchQuery: QueryType,
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

    val baseRequest = buildBaseRequest(
      searchQuery,
      domains,
      searchContext,
      categories,
      tags,
      only,
      boosts,
      minShouldMatch,
      slop,
      functionScores
    )

    // First pass logic is very simple. advanced query >> query >> categories >> tags
    val sort = (searchQuery, categories, tags) match {
      case (NoQuery, None, None) =>
        SortBuilders
          .scoreSort()
          .order(SortOrder.DESC)

      // Query
      case (AdvancedQuery(_) | SimpleQuery(_), _, _)  =>
        SortBuilders
          .scoreSort()
          .order(SortOrder.DESC)

      // Categories
      case (_, Some(cats), _) if searchContext.isEmpty =>
        SortBuilders
          .fieldSort("animl_annotations.categories.score")
          .order(SortOrder.DESC)
          .sortMode("avg")
          .setNestedFilter(FilterBuilders.termsFilter(CategoriesFieldType.rawFieldName, cats.toSeq:_*))

      // Categories and search context
      // TODO: Should we sort by popularity?
      case (_, Some(cats), _) if searchContext.isDefined =>
        SortBuilders
          .fieldSort(TitleFieldType.fieldName)
          .order(SortOrder.DESC)

      // Tags
      case (_, _, Some(ts)) =>
        SortBuilders
          .fieldSort("animl_annotations.tags.score")
          .order(SortOrder.DESC)
          .sortMode("avg")
          .setNestedFilter(FilterBuilders.termsFilter(TagsFieldType.rawFieldName, ts.toSeq:_*))
    }

    baseRequest
      .setFrom(offset)
      .setSize(limit)
      .addSort(sort)
  }

  def buildCountRequest(field: CeteraFieldType with Countable,
                        searchQuery: QueryType,
                        domains: Option[Set[String]],
                        searchContext: Option[String],
                        categories: Option[Set[String]],
                        tags: Option[Set[String]],
                        only: Option[String]): SearchRequestBuilder = {

    val baseRequest = buildBaseRequest(
      searchQuery,
      domains,
      searchContext,
      categories,
      tags,
      only,
      Map.empty,
      None,
      None,
      List.empty
    )

    val aggregation = field match {
      case DomainFieldType =>
        AggregationBuilders
          .terms("domains")
          .field(field.rawFieldName)
          .order(Terms.Order.count(false)) // count desc
          .size(0) // unlimited

      case CategoriesFieldType =>
        AggregationBuilders
          .nested("annotations")
          .path(field.fieldName)
          .subAggregation(
            AggregationBuilders
              .terms("names")
              .field(field.rawFieldName)
              .size(0)
          )

      case TagsFieldType =>
        AggregationBuilders
          .nested("annotations")
          .path(field.fieldName)
          .subAggregation(
            AggregationBuilders
              .terms("names")
              .field(field.rawFieldName)
              .size(0)
          )

      case CustomerCategoryFieldType =>
        AggregationBuilders
          .terms("categories")
          .field(field.rawFieldName)
          .order(Terms.Order.count(false)) // count desc
          .size(0) // unlimited
    }

    baseRequest
      .addAggregation(aggregation)
      .setSearchType("count")
  }
}
