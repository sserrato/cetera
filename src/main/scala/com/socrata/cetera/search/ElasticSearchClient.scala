package com.socrata.cetera.search

import java.io.Closeable

import org.elasticsearch.action.search.{SearchRequestBuilder, SearchResponse}
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.index.query.{FilterBuilders, QueryBuilders}
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.elasticsearch.search.sort.{SortBuilders, SortOrder}

import com.socrata.cetera.types.CeteraFieldType
import com.socrata.cetera.types._

// Elastic Search Field Translator
//
// There is some confusion here because semantic types and implementation details are out of sync.
// categories and tags are nested in ES whereas domain_cname is not.
object ESFT {
  def fieldName(field: CeteraFieldType): String = {
    field match {
      case DomainFieldType => "socrata_id.domain_cname"

      case CategoriesFieldType => "animl_annotations.categories"
      case TagsFieldType => "animl_annotations.tags"

      case TitleFieldType => "indexed_metadata.name"
      case DescriptionFieldType => "indexed_metadata.description"
    }
  }

  def rawFieldName(field: CeteraFieldType with Countable): String = {
    field match {
      case DomainFieldType => fieldName(field) + ".raw"
      case CategoriesFieldType => fieldName(field) + ".name.raw"
      case TagsFieldType => fieldName(field) + ".name.raw"
    }
  }

  // NOTE: domain_cname does not currently have a score associated with it
  def scoreFieldName(field: CeteraFieldType with Countable): String = {
    fieldName(field) + ".score"
  }
}

class ElasticSearchClient(host: String, port: Int, clusterName: String) extends Closeable {
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
        QueryBuilders.matchQuery("_all", sq)

      case Some(sq) =>
        val text_args = boosts.map {
          case (field, weight) =>
            val fieldName = ESFT.fieldName(field)
            s"${fieldName}^${weight}" // NOTE ^ does not mean exponentiate, it means multiply
        } ++ List("_all")

        QueryBuilders.multiMatchQuery(sq, text_args.toList:_*)
    }

    val query = locally {
      val domainFilter = domains.map { domains =>
        FilterBuilders.termsFilter(ESFT.rawFieldName(DomainFieldType), domains.toSeq:_*)
      }

      val categoriesFilter = categories.map { categories =>
        FilterBuilders.nestedFilter(ESFT.fieldName(CategoriesFieldType),
          FilterBuilders.termsFilter(ESFT.rawFieldName(CategoriesFieldType), categories.toSeq:_*))
      }

      val tagsFilter = tags.map { tags =>
        FilterBuilders.nestedFilter(ESFT.fieldName(TagsFieldType),
          FilterBuilders.termsFilter(ESFT.rawFieldName(TagsFieldType), tags.toSeq:_*))
      }

      val filters = List(domainFilter, categoriesFilter, tagsFilter).flatten

      if (filters.nonEmpty) {
        QueryBuilders.filteredQuery(matchQuery, FilterBuilders.andFilter(filters:_*))
      } else {
        matchQuery
      }
    }

    // Imperative builder --> order is important
    client
      .prepareSearch("datasets", "pages") // literals should not be here
      .setTypes(only.toList:_*)
      .setQuery(query)
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
          .sortMode("max")
          .setNestedFilter(FilterBuilders.termsFilter(ESFT.rawFieldName(CategoriesFieldType), cats.toSeq:_*))

      // Tags
      case (_, _, Some(ts)) =>
        SortBuilders
          .fieldSort("animl_annotations.tags.score")
          .order(SortOrder.DESC)
          .sortMode("max")
          .setNestedFilter(FilterBuilders.termsFilter(ESFT.rawFieldName(TagsFieldType), ts.toSeq:_*))
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
          .field(ESFT.rawFieldName(field))
          .order(Terms.Order.count(false)) // count desc
          .size(0) // unlimited

      case CategoriesFieldType =>
        AggregationBuilders
          .nested("annotations")
          .path(ESFT.fieldName(field))
          .subAggregation(
            AggregationBuilders
              .terms("names")
              .field(ESFT.rawFieldName(field))
          )

      case TagsFieldType =>
        AggregationBuilders
          .nested("annotations")
          .path(ESFT.fieldName(field))
          .subAggregation(
            AggregationBuilders
              .terms("names")
              .field(ESFT.rawFieldName(field))
          )
    }

    baseRequest
      .addAggregation(aggregation)
      .setSearchType("count")
  }
}
