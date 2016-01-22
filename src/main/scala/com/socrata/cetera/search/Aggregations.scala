package com.socrata.cetera.search

import org.elasticsearch.search.aggregations.{AbstractAggregationBuilder, AggregationBuilders}
import org.elasticsearch.search.aggregations.bucket.terms.Terms

import com.socrata.cetera.types._

object Aggregations {
  // The 'terms' and 'nested' fields need to jive with
  // ../services/CountService.scala

  val domains =
    AggregationBuilders
      .terms("domains")
      .field(DomainFieldType.rawFieldName)
      .order(Terms.Order.count(false)) // count desc
      .size(0) // no docs, aggs only

  val domainCategories =
    AggregationBuilders
      .terms("domain_categories")
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
      case DomainFieldType => Aggregations.domains
      case DomainCategoryFieldType => Aggregations.domainCategories

      case CategoriesFieldType => Aggregations.categories
      case TagsFieldType => Aggregations.tags
    }
}
