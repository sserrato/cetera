package com.socrata.cetera.search

import org.elasticsearch.index.query.FilterBuilders
import org.elasticsearch.search.aggregations.{AbstractAggregationBuilder, AggregationBuilders}
import org.elasticsearch.search.aggregations.bucket.terms.Terms

import com.socrata.cetera._
import com.socrata.cetera.types._

object Aggregations {
  // The 'terms' and 'nested' fields need to jive with
  // ../services/CountService.scala

  val domains =
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
              .filter(
                FilterBuilders.boolFilter()
                  .must(Filters.customerDomainFilter.get)
                  .must(Filters.moderationStatusFilter().get)
                  .must(Filters.routingApprovalFilter(None, isDomainAgg = true).get)
              )
          )
      )

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

  def chooseAggregation(field: CeteraFieldType with Countable with Rawable) : AbstractAggregationBuilder =
    field match {
      case DomainCnameFieldType => Aggregations.domains
      case DomainCategoryFieldType => Aggregations.domainCategories
      case DomainTagsFieldType => Aggregations.domainTags

      case CategoriesFieldType => Aggregations.categories
      case TagsFieldType => Aggregations.tags
    }
}
