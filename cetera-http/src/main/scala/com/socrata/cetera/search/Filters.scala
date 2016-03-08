package com.socrata.cetera.search

import org.elasticsearch.index.query.FilterBuilders._
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.index.query.{FilterBuilder, QueryBuilder}

import com.socrata.cetera._
import com.socrata.cetera.types._

object DocumentQueries {
  def categoriesQuery(categories: Option[Set[String]]): Option[QueryBuilder] =
    categories.map { cs =>
      nestedQuery(
        CategoriesFieldType.fieldName,
        cs.foldLeft(boolQuery().minimumNumberShouldMatch(1)) { (b, q) =>
          b.should(matchQuery(CategoriesFieldType.Name.fieldName, q))
        }
      )
    }

  def tagsQuery(tags: Option[Set[String]]): Option[QueryBuilder] =
    tags.map { tags =>
      nestedQuery(
        TagsFieldType.fieldName,
        tags.foldLeft(boolQuery().minimumNumberShouldMatch(1)) { (b, q) =>
          b.should(matchQuery(TagsFieldType.Name.fieldName, q))
        }
      )
    }

  def domainCategoriesQuery(categories: Option[Set[String]]): Option[QueryBuilder] =
    categories.map { cs =>
      cs.foldLeft(boolQuery().minimumNumberShouldMatch(1)) { (b, q) =>
        b.should(matchQuery(DomainCategoryFieldType.fieldName, q))
      }
    }

  def domainTagsQuery(tags: Option[Set[String]]): Option[QueryBuilder] =
    tags.map { ts =>
      ts.foldLeft(boolQuery().minimumNumberShouldMatch(1)) { (b, q) =>
        b.should(matchQuery(DomainTagsFieldType.fieldName, q))
      }
    }
}

object DocumentFilters {
  def datatypeFilter(datatypes: Option[Seq[String]], aggPrefix: String = ""): Option[FilterBuilder] =
    datatypes.map(ts => datatypeFilter(ts, aggPrefix))

  def datatypeFilter(datatypes: Seq[String], aggPrefix: String): FilterBuilder = {
    val validatedDatatypes = datatypes.flatMap(t => Datatype(t).map(_.singular))
    termsFilter(aggPrefix + DatatypeFieldType.fieldName, validatedDatatypes: _*)
  }

  def domainIdsFilter(domainIds: Set[Int], aggPrefix: String = ""): Option[FilterBuilder] =
    if (domainIds.nonEmpty) {
      Some(termsFilter(aggPrefix + SocrataIdDomainIdFieldType.fieldName, domainIds.toSeq: _*))
    } else { None }

  def domainIdFilter(domainId: Int, aggPrefix: String = ""): Option[FilterBuilder] =
    domainIdsFilter(Set(domainId), aggPrefix)

  def isApprovedByParentDomainFilter(aggPrefix: String = ""): Option[FilterBuilder] =
    Some(termFilter(aggPrefix + "is_approved_by_parent_domain", true))

  def domainMetadataFilter(metadata: Option[Set[(String, String)]]): Option[FilterBuilder] =
    metadata.map { ss =>
      orFilter(
        ss.map { case (key, value) =>
          nestedFilter(
            DomainMetadataFieldType.fieldName,
            andFilter(
              termsFilter(DomainMetadataFieldType.Key.rawFieldName, key),
              termsFilter(DomainMetadataFieldType.Value.rawFieldName, value)
            )
          )
        }.toSeq: _*
      )
    }

  /**
    * Filter to only show documents that have passed the view moderation workflow.
    *
    * in general a document is visible when either:
    *   it is a default view,
    *   moderation is approved,
    *   parent domain moderation is disabled.
    * datalens has a special treatment: **LENS**
    *   it is a default view, (which may never occur) or
    *   moderation is approved,
    *   we usually show more things when parent domain moderation disabled, but not datalenses!
    * federation is mostly the same but has one special treatment: **FED**
    *   when search context is moderated, and parent domain is not moderated: only show defaults!
    *
    * @param searchContextIsModerated is the catalog search context view moderated?
    * @return composable filter builder
    */
  def moderationStatusFilter(searchContextIsModerated: Boolean,
                             moderatedDomainIds: Set[Int],
                             unmoderatedDomainIds: Set[Int],
                             isDomainAgg: Boolean = false): FilterBuilder = {
    val aggPrefix = if (isDomainAgg) esDocumentType + "." else ""
    val documentIsDefault = termFilter(aggPrefix + IsDefaultViewFieldType.fieldName, true)
    val documentIsAccepted = termFilter(aggPrefix + IsModerationApprovedFieldType.fieldName, true)

    val parentDomainIsModerated = domainIdsFilter(moderatedDomainIds, aggPrefix)
    val parentDomainIsNotModerated = domainIdsFilter(unmoderatedDomainIds, aggPrefix)

    val datalensUniqueAndSpecialSnowflakeFilter = datatypeFilter(Seq(TypeDatalenses.singular), aggPrefix)
    val documentIsNotDatalens = notFilter(datalensUniqueAndSpecialSnowflakeFilter)

    val basicFilter = boolFilter()
      .should(documentIsDefault)
      .should(documentIsAccepted)
    parentDomainIsNotModerated.foreach(f => basicFilter.should(
      boolFilter()
        .must(f)
        .must(documentIsNotDatalens)
    ))

    val contextualFilter = boolFilter()
    parentDomainIsModerated.foreach(f => contextualFilter.should(
      boolFilter()
        .must(f)
        .must(boolFilter()
          .should(documentIsDefault)
          .should(documentIsAccepted)
        )
    ))
    parentDomainIsNotModerated.foreach(f => contextualFilter.should(
      boolFilter()
        .must(f)
        .must(documentIsDefault)
    ))

    if (searchContextIsModerated) contextualFilter else basicFilter
  }

  /**
    * Filter to only show or aggregate documents that have passed the routing & approval workflow
    *
    * in general a document is visible when either:
    *   parent domain R&A is disabled, or
    *   document is approved by parent domain.
    * federation additionally requires that either:
    *   search context domain R&A is disabled, or
    *   document is approved by search context domain.
    *
    * @param searchContext the catalog search context
    * @param isDomainAgg is the search an aggregation on domains
    * @return composable filter builder
    */
  def routingApprovalFilter(searchContext: Option[Domain],
                            raOffDomainIds: Set[Int],
                            isDomainAgg: Boolean = false): FilterBuilder = {
    val prefix = if (isDomainAgg) esDocumentType + "." else ""

    val documentIsApprovedBySearchContext = searchContext.flatMap { d =>
      if (d.routingApprovalEnabled) {
        Some(termsFilter(ApprovingDomainIdsFieldType.fieldName, d.domainId))
      } else { None }
    }

    val documentRAVisible = boolFilter()
    domainIdsFilter(raOffDomainIds, prefix).foreach(documentRAVisible.should)
    isApprovedByParentDomainFilter(prefix).foreach(documentRAVisible.should)

    val filter = boolFilter()
    documentIsApprovedBySearchContext.foreach(filter.must)
    filter.must(documentRAVisible)
    filter
  }

  def publicFilter(isDomainAgg: Boolean = false): FilterBuilder = {
    val prefix = if (isDomainAgg) esDocumentType + "." else ""
    notFilter(termFilter(prefix + IsPublicFieldType.fieldName, false))
  }
}

object DomainFilters {
  def domainIds(domainIds: Set[Int]): FilterBuilder = termsFilter("domain_id", domainIds.toSeq: _*)

  // two nos make a yes: this filters out items with is_customer_domain=false, while permitting true or null.
  def isNotCustomerDomainFilter: FilterBuilder = termFilter(IsCustomerDomainFieldType.fieldName, false)
  def isCustomerDomainFilter: FilterBuilder = notFilter(isNotCustomerDomainFilter)
}
