package com.socrata.cetera.search

import java.io.Closeable
import scala.collection.JavaConverters._

import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.index.query.MultiMatchQueryBuilder
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders
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
  def buildBaseRequest(searchQuery: Option[String],
                       domains: Option[Set[String]],
                       categories: Option[Set[String]],
                       tags: Option[Set[String]],
                       only: Option[String],
                       boosts: Map[CeteraFieldType with Boostable, Float]): SearchRequestBuilder = {

    val matchQuery = searchQuery match {
      case None =>
        QueryBuilders.matchAllQuery

      case Some(sq) if boosts.isEmpty =>
        QueryBuilders.multiMatchQuery(sq, "_all")
          .`type`(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
          .analyzer("snowball")

      case Some(sq) =>
        val text_args = boosts.map {
          case (field, weight) =>
            val fieldName = field.fieldName
            s"${fieldName}^${weight}" // NOTE ^ does not mean exponentiate, it means multiply
        } ++ List("_all")

        QueryBuilders.multiMatchQuery(sq, text_args.toList:_*)
          .`type`(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
          .analyzer("snowball")
    }

    val query = locally {
      val domainFilter = domains.map { domains =>
        FilterBuilders.termsFilter(DomainFieldType.rawFieldName, domains.toSeq:_*)
      }

      val categoriesFilter = categories.map { categories =>
        FilterBuilders.nestedFilter(CategoriesFieldType.fieldName,
          FilterBuilders.termsFilter(CategoriesFieldType.rawFieldName, categories.toSeq:_*))
      }

      val tagsFilter = tags.map { tags =>
        FilterBuilders.nestedFilter(TagsFieldType.fieldName,
          FilterBuilders.termsFilter(TagsFieldType.rawFieldName, tags.toSeq:_*))
      }

      val filters = List(domainFilter, categoriesFilter, tagsFilter).flatten

      if (filters.nonEmpty) {
        QueryBuilders.filteredQuery(matchQuery, FilterBuilders.andFilter(filters:_*))
      } else {
        matchQuery
      }
    }

    // Imperative builder --> order is important
    val finalQuery = client
      .prepareSearch("datasets", "pages") // literals should not be here
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

  def buildSearchRequest(searchQuery: Option[String],
                         domains: Option[Set[String]],
                         categories: Option[Set[String]],
                         tags: Option[Set[String]],
                         only: Option[String],
                         boosts: Map[CeteraFieldType with Boostable, Float],
                         offset: Int,
                         limit: Int): SearchRequestBuilder = {

    val baseRequest = buildBaseRequest(
      searchQuery,
      domains,
      categories,
      tags,
      only,
      boosts
    )

    // First pass logic is very simple. query >> categories >> tags
    val sort = (searchQuery, categories, tags) match {
      case (None, None, None) =>
        SortBuilders
          .scoreSort()
          .order(SortOrder.DESC)

      // Query
      case (Some(sq), _, _) =>
        SortBuilders
          .scoreSort()
          .order(SortOrder.DESC)

      // Categories
      case (_, Some(cats), _) =>
        SortBuilders
          .fieldSort("animl_annotations.categories.score")
          .order(SortOrder.DESC)
          .sortMode("avg")
          .setNestedFilter(FilterBuilders.termsFilter(CategoriesFieldType.rawFieldName, cats.toSeq:_*))

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
                        searchQuery: Option[String],
                        domains: Option[Set[String]],
                        categories: Option[Set[String]],
                        tags: Option[Set[String]],
                        only: Option[String]): SearchRequestBuilder = {

    val baseRequest = buildBaseRequest(
      searchQuery,
      domains,
      categories,
      tags,
      only,
      Map.empty
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
    }

    baseRequest
      .addAggregation(aggregation)
      .setSearchType("count")
  }
}
