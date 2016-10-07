package com.socrata.cetera.search

import org.elasticsearch.index.query.FilterBuilders
import org.elasticsearch.index.query.FilterBuilders.boolFilter
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.elasticsearch.search.aggregations.{AbstractAggregationBuilder, AggregationBuilders}

import com.socrata.cetera.auth.User
import com.socrata.cetera.esDocumentType
import com.socrata.cetera.types._

object DocumentAggregations {
  // The 'terms' used in the AggregationBuilders below need to be accounted for in the CountService val 'pattern'.

  private val aggSize = 0 // agg count unlimited

  val domainCategories =
    AggregationBuilders
      .terms("domain_categories")
      .field(DomainCategoryFieldType.rawFieldName)
      .order(Terms.Order.count(false)) // count desc
      .size(aggSize)

  val domainTags =
    AggregationBuilders
      .terms("domain_tags")
      .field(DomainTagsFieldType.rawFieldName)
      .order(Terms.Order.count(false)) // count desc
      .size(aggSize)

  val categories =
    AggregationBuilders
      .nested("annotations")
      .path(CategoriesFieldType.fieldName)
      .subAggregation(
        AggregationBuilders
          .terms("names")
          .field(CategoriesFieldType.Name.rawFieldName)
          .size(aggSize)
      )

  val tags =
    AggregationBuilders
      .nested("annotations")
      .path(TagsFieldType.fieldName)
      .subAggregation(
        AggregationBuilders
          .terms("names")
          .field(TagsFieldType.Name.rawFieldName)
          .size(aggSize)
      )

  val owners =
    AggregationBuilders
      .terms("owners")
      .field(OwnerIdFieldType.rawFieldName)
      .order(Terms.Order.count(false)) // count desc
      .size(aggSize)

  val attributions =
    AggregationBuilders
      .terms("attributions")
      .field(AttributionFieldType.rawFieldName)
      .order(Terms.Order.count(false)) // count desc
      .size(aggSize)


  def chooseAggregation(field: DocumentFieldType with Countable with Rawable): AbstractAggregationBuilder =
    field match {
      case DomainCategoryFieldType => domainCategories
      case DomainTagsFieldType => domainTags

      case CategoriesFieldType => categories
      case TagsFieldType => tags

      case OwnerIdFieldType => owners
      case AttributionFieldType => attributions
    }
}

object DomainAggregations {
  private val aggSize = 0 // agg count unlimited

  def domains(domainSet: DomainSet, user: Option[User]): AbstractAggregationBuilder = {
    val visibilityFilter = boolFilter().must(DocumentFilters.anonymousFilter(domainSet, isDomainAgg = true))

    AggregationBuilders
      .terms("domains") // "domains" is an agg of terms on field "domain_cname.raw"
      .field("domain_cname.raw")
      .size(aggSize)
      .subAggregation(
        AggregationBuilders
          .children("documents") // "documents" is an agg of children of type esDocumentType
          .childType(esDocumentType)
          .subAggregation(
            AggregationBuilders
              .filter("visible") // "visible" is an agg of documents matching the following filter
              .filter(visibilityFilter)
          )
      )
  }
}
