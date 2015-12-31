package com.socrata.cetera.search

import org.elasticsearch.index.query._

import com.socrata.cetera.types._

object Filters {
  def datatypeFilter(datatypes: Option[Seq[String]]): Option[TermsFilterBuilder] =
    datatypes.map { ts =>
      val validatedDatatypes = ts.map(t => Datatype(t).map(_.singular)).flatten
      FilterBuilders.termsFilter(DatatypeFieldType.fieldName, validatedDatatypes: _*)
    }

  def domainFilter(domains: Option[Set[String]]): Option[TermsFilterBuilder] =
    domains.map { ds => FilterBuilders.termsFilter(DomainFieldType.rawFieldName, ds.toSeq: _*) }

  def categoriesFilter(categories: Option[Set[String]]): Option[NestedFilterBuilder] =
    categories.map { cs =>
      FilterBuilders.nestedFilter(
        CategoriesFieldType.fieldName,
        FilterBuilders.termsFilter(CategoriesFieldType.Name.rawFieldName, cs.toSeq: _*)
      )
    }

  def tagsFilter(tags: Option[Set[String]]): Option[NestedFilterBuilder] =
    tags.map { tags =>
      FilterBuilders.nestedFilter(
        TagsFieldType.fieldName,
        FilterBuilders.termsFilter(TagsFieldType.Name.rawFieldName, tags.toSeq: _*)
      )
    }

  def domainCategoriesFilter(categories: Option[Set[String]]): Option[TermsFilterBuilder] =
    categories.map { cs =>
      FilterBuilders.termsFilter(DomainCategoryFieldType.rawFieldName, cs.toSeq: _*)
    }

  def domainTagsFilter(tags: Option[Set[String]]): Option[TermsFilterBuilder] =
    tags.map { ts =>
      FilterBuilders.termsFilter(DomainTagsFieldType.rawFieldName, ts.toSeq: _*)
    }

  def domainMetadataFilter(metadata: Option[Set[(String, String)]]): Option[OrFilterBuilder] =
    metadata.map { ss =>
      FilterBuilders.orFilter(
        ss.map { case (key, value) =>
          FilterBuilders.nestedFilter(
            DomainMetadataFieldType.fieldName,
            FilterBuilders.andFilter(
              FilterBuilders.termsFilter(DomainMetadataFieldType.Key.rawFieldName, key),
              FilterBuilders.termsFilter(DomainMetadataFieldType.Value.rawFieldName, value)
            )
          )
        }.toSeq: _*
      )
    }

  def customerDomainFilter: Option[NotFilterBuilder] =
    Some(FilterBuilders.notFilter(FilterBuilders.termsFilter(IsCustomerDomainFieldType.fieldName, "false")))

  def moderationStatusFilter(domainModerated:Boolean = false): Option[NotFilterBuilder] = {
    val unwantedViews =
      if (domainModerated) {
        FilterBuilders.termsFilter(ModerationStatusFieldType.fieldName, "pending", "rejected", "not_moderated")
      } else {
        FilterBuilders.termsFilter(ModerationStatusFieldType.fieldName, "pending", "rejected")
      }
    Some(FilterBuilders.notFilter(unwantedViews))
  }

  def routingApprovalFilter(domain: Option[Domain]): Option[TermsFilterBuilder] = {
    domain.flatMap { d =>
      if (d.routingApprovalEnabled) {
        Some(FilterBuilders.termsFilter(ApprovingDomainsFieldType.fieldName, d.domainCname))
      } else {
        None
      }
    }
  }
}
