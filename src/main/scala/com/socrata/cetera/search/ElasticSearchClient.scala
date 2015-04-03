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

import com.socrata.cetera.types.CeteraFieldType
import com.socrata.cetera.types._

object ElasticSearchFieldTranslator {
  def getFieldName(field: CeteraFieldType): String = {
    field match {
      case DomainFieldType => "socrata_id.domain_cname.raw"
      case CategoriesFieldType => "animl_annotations.category_names.raw"
      case TagsFieldType => "animl_annotations.tag_names.raw"

      case TitleFieldType => "indexed_metadata.name"
      case DescriptionFieldType => "indexed_metadata.description"
    }
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
                       domains: Set[String],
                       categories: Set[String],
                       tags: Set[String],
                       only: Option[String],
                       boosts: Map[CeteraFieldType, Float]): SearchRequestBuilder = {

    val matchQuery = searchQuery match {
      case None =>
        QueryBuilders.matchAllQuery
      case Some(sq) if boosts.isEmpty =>
        QueryBuilders.matchQuery("_all", sq)
      case Some(sq) =>
        val text_args = boosts.map {
          case (field, weight) =>
            val fieldName = ElasticSearchFieldTranslator.getFieldName(field)
            s"${fieldName}^${weight}" // NOTE ^ does not mean exponentiate, it means multiply
        } ++ List("_all")

        QueryBuilders.multiMatchQuery(sq, text_args.toList:_*)
    }

    val query = locally {
        val fieldTypeTerms = List(
          DomainFieldType -> domains,
          CategoriesFieldType -> categories,
          TagsFieldType -> tags
        ).filter(_._2.nonEmpty)

        val filters = fieldTypeTerms.map { case (fieldType, terms) =>
          FilterBuilders.termsFilter(
            ElasticSearchFieldTranslator.getFieldName(fieldType),
            terms.toSeq:_*
          )
        }

        if(filters.nonEmpty) {
          QueryBuilders.filteredQuery(
            matchQuery,
            FilterBuilders.andFilter(filters:_*)
          )
        } else {
          matchQuery
        }
    }

    // Imperative builder --> order is important
    client
      .prepareSearch("datasets", "pages")
      .setTypes(only.toList:_*)
      .setQuery(query)
  }

  def buildSearchRequest(searchQuery: Option[String],
                         domains: Set[String],
                         categories: Set[String],
                         tags: Set[String],
                         only: Option[String],
                         boosts: Map[CeteraFieldType, Float],
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

    baseRequest
      .setFrom(offset)
      .setSize(limit)
  }

  def buildCountRequest(field: CeteraFieldType,
                        searchQuery: Option[String],
                        domains: Set[String],
                        categories: Set[String],
                        tags: Set[String],
                        only: Option[String]): SearchRequestBuilder = {

    val baseRequest = buildBaseRequest(
      searchQuery,
      domains,
      categories,
      tags,
      only,
      Map.empty
    )

    val aggregation = AggregationBuilders
      .terms("counts")
      .field(ElasticSearchFieldTranslator.getFieldName(field))
      .order(Terms.Order.count(false)) // count desc
      .size(0) // unlimited

    baseRequest
      .addAggregation(aggregation)
      .setSearchType("count")
  }
}
