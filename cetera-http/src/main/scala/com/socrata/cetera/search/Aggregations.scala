package com.socrata.cetera.search

import org.elasticsearch.index.query.FilterBuilders
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.elasticsearch.search.aggregations.{AbstractAggregationBuilder, AggregationBuilders}

import com.socrata.cetera._
import com.socrata.cetera.types._

object DocumentAggregations {
  // The 'terms' and 'nested' fields need to jive with
  // ../services/CountService.scala

  val domainCategories =
    AggregationBuilders
      .terms("domain_categories")
      .field(DomainCategoryFieldType.rawFieldName)
      .order(Terms.Order.count(false)) // count desc
      .size(0) // no docs, aggs only

  val domainTags =
    AggregationBuilders
      .terms("domain_tags")
      .field(DomainCategoryFieldType.rawFieldName)
      .order(Terms.Order.count(false)) // count desc
      .size(0) // no docs, aggs only

  val categories =
    AggregationBuilders
      .nested("annotations")
      .path(CategoriesFieldType.fieldName)
      .subAggregation(
        AggregationBuilders
          .terms("names")
          .field(CategoriesFieldType.Name.rawFieldName)
          .size(0)
      )

  val tags =
    AggregationBuilders
      .nested("annotations")
      .path(TagsFieldType.fieldName)
      .subAggregation(
        AggregationBuilders
          .terms("names")
          .field(TagsFieldType.Name.rawFieldName)
          .size(0)
      )

  def chooseAggregation(field: CeteraFieldType with Countable with Rawable): AbstractAggregationBuilder =
    field match {
      case DomainCategoryFieldType => domainCategories
      case DomainTagsFieldType => domainTags
      case CategoriesFieldType => categories
      case TagsFieldType => tags
      case _ => ???
    }
}

object DomainAggregations {
  def domains(searchContextIsMod: Boolean,
              modDomainIds: Set[Int],
              unmodDomainIds: Set[Int],
              raOffDomainIds: Set[Int]): AbstractAggregationBuilder = {
    val moderationFilter =
      DocumentFilters.moderationStatusFilter(searchContextIsMod, modDomainIds, unmodDomainIds, isDomainAgg = true)
    val routingApprovalFilter =
      DocumentFilters.routingApprovalFilter(None, raOffDomainIds, isDomainAgg = true)
    AggregationBuilders
      .terms("domains") // "domains" is an agg of terms on field "domain_cname.raw"
      .field("domain_cname.raw")
      .subAggregation(
        AggregationBuilders
          .children("documents") // "documents" is an agg of children of type esDocumentType
          .childType(esDocumentType)
          .subAggregation(
            AggregationBuilders
              .filter("visible") // "visible" is an agg of documents matching the following filter
              .filter(FilterBuilders.boolFilter()
                // is customer domain filter must be applied above this aggregation
                // apply moderation filter
                .must(moderationFilter)
                // apply routing & approval filter
                .must(routingApprovalFilter)
              )
          )
      )
  }
}
