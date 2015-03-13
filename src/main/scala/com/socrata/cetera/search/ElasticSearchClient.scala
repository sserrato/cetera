package com.socrata.cetera.search

import java.io.Closeable

import org.elasticsearch.action.search.{SearchRequestBuilder, SearchResponse}
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.index.query.{FilterBuilders, QueryBuilders}
import org.elasticsearch.search.aggregations.AggregationBuilders

class ElasticSearchClient(host: String, port: Int, clusterName: String) extends Closeable {
  val settings = ImmutableSettings.settingsBuilder()
                   .put("cluster.name", clusterName)
                   .put("client.transport.sniff", true)
                   .build()

  val client: Client = new TransportClient(settings)
    .addTransportAddress(new InetSocketTransportAddress(host, port))

  def close(): Unit = client.close()

  // Assumes query extraction and validation have already been done
  def buildRequest(searchQuery: Option[String],
                   domains: Option[Set[String]],
                   categories: Option[Set[String]],
                   tags: Option[Set[String]],
                   only: Option[String]): SearchRequestBuilder = {

    val filteredQuery = {
      val matchQuery = searchQuery match {
        case None => QueryBuilders.matchAllQuery()
        case Some(sq) => QueryBuilders.matchQuery("_all", sq)
      }

      // OR of domain filters
      val domainFilter = domains match {
        case None => FilterBuilders.matchAllFilter()
        case Some(d) => FilterBuilders.termsFilter("socrata_id.domain_cname.raw", d.toSeq:_*)
      }

      // OR of category filters
      val categoryFilter = categories match {
        case None => FilterBuilders.matchAllFilter()
        case Some(c) => FilterBuilders.termsFilter("animl_annotations.category_names.raw", c.toSeq:_*)
      }

      // OR of tag filters
      val tagFilter = tags match {
        case None => FilterBuilders.matchAllFilter()
        case Some(c) => FilterBuilders.termsFilter("animl_annotations.tag_names.raw", c.toSeq:_*)
      }

      // AND of OR filters (CNF)
      val domainAndCategoryFilter = FilterBuilders.andFilter(
        domainFilter,
        categoryFilter,
        tagFilter
      )

      QueryBuilders.filteredQuery(
        matchQuery,
        domainAndCategoryFilter
      )
    }

    // Imperative builder --> order is important
    client
      .prepareSearch()
      .setTypes(only.toList:_*)
      .setQuery(filteredQuery)
  }

  def buildSearchRequest(searchQuery: Option[String],
                         domains: Option[Set[String]],
                         categories: Option[Set[String]],
                         tags: Option[Set[String]],
                         only: Option[String],
                         offset: Int,
                         limit: Int): SearchRequestBuilder = {

    val baseRequest = buildRequest(
      searchQuery,
      domains,
      categories,
      tags,
      only
    )

    baseRequest
      .setFrom(offset)
      .setSize(limit)
  }

  def buildDomainRequest(searchQuery: Option[String],
                         domains: Option[Set[String]],
                         categories: Option[Set[String]],
                         tags: Option[Set[String]],
                         only: Option[String],
                         offset: Int,
                         limit: Int): SearchRequestBuilder = {

    val baseRequest = buildRequest(
      searchQuery,
      domains,
      categories,
      tags,
      only
    )

    val aggregation = AggregationBuilders
      .terms("resources_by_domain_count")
      .field("socrata_id.domain_cname.raw")

    baseRequest
      .addAggregation(aggregation)
      .setFrom(offset)
      .setSize(limit)
  }
}
