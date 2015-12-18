package com.socrata.cetera.search

import org.elasticsearch.index.query._

import com.socrata.cetera.types._

object Filters {
  def datatypeFilter(datatypes: Option[Seq[String]]): Option[TermsQueryBuilder] =
    datatypes.map { ts =>
      val validatedDatatypes = ts.map(t => DatatypeSimple(t).map(_.singular)).flatten
      QueryBuilders.termsQuery(DatatypeFieldType.fieldName, validatedDatatypes: _*)
    }

  def domainFilter(domains: Option[Set[String]]): Option[TermsQueryBuilder] =
    domains.map { ds => QueryBuilders.termsQuery(DomainFieldType.rawFieldName, ds.toSeq: _*) }

  def categoriesFilter(categories: Option[Set[String]]): Option[NestedQueryBuilder] =
    categories.map { cs =>
      QueryBuilders.nestedQuery(
        CategoriesFieldType.fieldName,
        QueryBuilders.termsQuery(CategoriesFieldType.Name.rawFieldName, cs.toSeq: _*)
      )
    }

  def tagsFilter(tags: Option[Set[String]]): Option[NestedQueryBuilder] =
    tags.map { tags =>
      QueryBuilders.nestedQuery(
        TagsFieldType.fieldName,
        QueryBuilders.termsQuery(TagsFieldType.Name.rawFieldName, tags.toSeq: _*)
      )
    }

  def domainCategoriesFilter(categories: Option[Set[String]]): Option[TermsQueryBuilder] =
    categories.map { cs =>
      QueryBuilders.termsQuery(DomainCategoryFieldType.rawFieldName, cs.toSeq: _*)
    }

  def domainTagsFilter(tags: Option[Set[String]]): Option[TermsQueryBuilder] =
    tags.map { ts =>
      QueryBuilders.termsQuery(DomainTagsFieldType.rawFieldName, ts.toSeq: _*)
    }

  def domainMetadataFilter(metadata: Option[Set[(String, String)]]): Option[BoolQueryBuilder] =
    metadata.map { sss =>
      sss.foldLeft(QueryBuilders.boolQuery().minimumNumberShouldMatch(1)) { (q, kvp) =>
        q.should(
          QueryBuilders.nestedQuery(
            DomainMetadataFieldType.fieldName,
            QueryBuilders.boolQuery()
              .must(QueryBuilders.termsQuery(DomainMetadataFieldType.Key.rawFieldName, kvp._1))
              .must(QueryBuilders.termsQuery(DomainMetadataFieldType.Value.rawFieldName, kvp._2))
          )
        )
      }
    }

  def customerDomainFilter: Option[NotQueryBuilder] =
    Some(QueryBuilders.notQuery(QueryBuilders.termsQuery(IsCustomerDomainFieldType.fieldName, "false")))

  def moderationStatusFilter(domainModerated:Boolean = false): Option[NotQueryBuilder] = {
    val unwantedViews =
      if (domainModerated) {
        QueryBuilders.termsQuery(ModerationStatusFieldType.fieldName, "pending", "rejected", "not_moderated")
      } else {
        QueryBuilders.termsQuery(ModerationStatusFieldType.fieldName, "pending", "rejected")
      }
    Some(QueryBuilders.notQuery(unwantedViews))
  }

  def routingApprovalFilter(domain: Option[Domain]): Option[TermsQueryBuilder] = {
    domain.flatMap { d =>
      if (d.routingApprovalEnabled) {
        Some(QueryBuilders.termsQuery(ApprovingDomainsFieldType.fieldName, d.domainCname))
      } else {
        None
      }
    }
  }
}
