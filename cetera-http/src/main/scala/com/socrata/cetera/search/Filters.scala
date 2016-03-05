package com.socrata.cetera.search

import org.elasticsearch.index.query.FilterBuilders._
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.index.query.{FilterBuilder, QueryBuilder}

import com.socrata.cetera._
import com.socrata.cetera.types._

object Filters {
  def datatypeFilter(datatypes: Option[Seq[String]]): Option[FilterBuilder] =
    datatypes.map { ts =>
      val validatedDatatypes = ts.flatMap(t => Datatype(t).map(_.singular))
      termsFilter(DatatypeFieldType.fieldName, validatedDatatypes: _*)
    }

  def domainIdFilter(domainIds: Set[Int]): Option[FilterBuilder] =
    if (domainIds.nonEmpty) Some(termsFilter(SocrataIdDomainIdFieldType.fieldName, domainIds.toSeq: _*)) else None

  def domainIdFilter(domainId: Int): Option[FilterBuilder] = domainIdFilter(Set(domainId))

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

  def customerDomainFilter: Option[FilterBuilder] =
    Some(hasParentFilter(esDomainType,
      notFilter(termFilter(IsCustomerDomainFieldType.fieldName, false))
    ))

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
  def moderationStatusFilter(searchContextIsModerated: Boolean = false): Option[FilterBuilder] = {
    val documentIsDefault = termFilter(IsDefaultViewFieldType.fieldName, true)
    val documentIsAccepted = termFilter(IsModerationApprovedFieldType.fieldName, true)

    val domainIsModerated = termFilter(IsModerationEnabledFieldType.fieldName, true)
    val parentDomainIsModerated = hasParentFilter(esDomainType, domainIsModerated)
    val parentDomainIsNotModerated = hasParentFilter(esDomainType, notFilter(domainIsModerated))

    val datalensUniqueAndSpecialSnowflakeFilter = termFilter(DatatypeFieldType.fieldName, TypeDatalenses.singular)
    val documentIsNotDatalens = notFilter(datalensUniqueAndSpecialSnowflakeFilter)

    val basicFilter = boolFilter()
      .should(documentIsDefault)
      .should(documentIsAccepted)
      .should(
        boolFilter()
          .must(parentDomainIsNotModerated)
          .must(documentIsNotDatalens) // **LENS**
      )

    val contextualFilter = boolFilter()
      .should(
        boolFilter()
          .must(parentDomainIsModerated)
          .must(basicFilter)
      )
      .should(
        boolFilter()
          .must(parentDomainIsNotModerated)
          .must(documentIsDefault) // **FED**
      )

    Some(if (searchContextIsModerated) contextualFilter else basicFilter)
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
  def routingApprovalFilter(searchContext: Option[Domain], isDomainAgg: Boolean = false): Option[FilterBuilder] = {
    val documentIsApprovedBySearchContext = searchContext.flatMap { d =>
      if (d.routingApprovalEnabled) {
        Some(termsFilter(ApprovingDomainIdsFieldType.fieldName, d.domainId))
      } else { None }
    }

    val parentDomainIsNotRA = hasParentFilter(esDomainType,
      notFilter(termFilter(IsRoutingApprovalEnabledFieldType.fieldName, true))
    )
    val prefix = if (isDomainAgg) esDocumentType + "." else ""
    val domainId = prefix + SocrataIdDomainIdFieldType.fieldName
    val approvingDomainIds = prefix + ApprovingDomainIdsFieldType.fieldName
    val documentIsApprovedByParentDomain = termFilter(prefix + "is_approved_by_parent_domain", true)
    val documentRAVisible = boolFilter()
      .should(parentDomainIsNotRA)
      .should(documentIsApprovedByParentDomain)

    val filter = boolFilter()
    documentIsApprovedBySearchContext.foreach(filter.must(_))
    filter.must(documentRAVisible)
    Some(filter)
  }
}
