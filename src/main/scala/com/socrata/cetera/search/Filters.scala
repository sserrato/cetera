package com.socrata.cetera.search

import org.elasticsearch.index.query._

import com.socrata.cetera.types._

object Filters {
  def datatypeFilter(datatypes: Option[Seq[String]]): Option[TermsFilterBuilder] =
    datatypes.map { ts =>
      val validatedDatatypes = ts.flatMap(t => Datatype(t).map(_.singular))
      FilterBuilders.termsFilter(DatatypeFieldType.fieldName, validatedDatatypes: _*)
    }

  def domainFilter(domains: Set[String]): Option[TermsFilterBuilder] =
    if (domains.nonEmpty) {
      Some(FilterBuilders.termsFilter(DomainFieldType.rawFieldName, domains.toSeq: _*))
    } else {
      None
    }

  def domainFilter(domain: String): Option[TermsFilterBuilder] =
    if (domain.nonEmpty) domainFilter(Set(domain)) else None

  def categoriesQuery(categories: Option[Set[String]]): Option[QueryBuilder] =
    categories.map { cs =>
      QueryBuilders.nestedQuery(
        CategoriesFieldType.fieldName,
        cs.foldLeft(QueryBuilders.boolQuery().minimumNumberShouldMatch(1)) { (b, q) =>
          b.should(QueryBuilders.matchQuery(CategoriesFieldType.Name.fieldName, q))
        }
      )
    }

  def tagsQuery(tags: Option[Set[String]]): Option[QueryBuilder] =
    tags.map { tags =>
      QueryBuilders.nestedQuery(
        TagsFieldType.fieldName,
        tags.foldLeft(QueryBuilders.boolQuery().minimumNumberShouldMatch(1)) { (b, q) =>
          b.should(QueryBuilders.matchQuery(TagsFieldType.Name.fieldName, q))
        }
      )
    }

  def domainCategoriesQuery(categories: Option[Set[String]]): Option[QueryBuilder] =
    categories.map { cs =>
      cs.foldLeft(QueryBuilders.boolQuery().minimumNumberShouldMatch(1)) { (b, q) =>
        b.should(QueryBuilders.matchQuery(DomainCategoryFieldType.fieldName, q))
      }
    }

  def domainTagsQuery(tags: Option[Set[String]]): Option[QueryBuilder] =
    tags.map { ts =>
      ts.foldLeft(QueryBuilders.boolQuery().minimumNumberShouldMatch(1)) { (b, q) =>
        b.should(QueryBuilders.matchQuery(DomainTagsFieldType.fieldName, q))
      }
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
