package com.socrata.cetera.search

import org.elasticsearch.index.query.{MultiMatchQueryBuilder, QueryBuilders}

import com.socrata.cetera.types.{FullTextSearchAnalyzedFieldType, FullTextSearchRawFieldType}

object SundryBuilders {
  def applyMinMatchConstraint(query: MultiMatchQueryBuilder, constraint: String): Unit =
    query.minimumShouldMatch(constraint)

  def applySlopParam(query: MultiMatchQueryBuilder, slop: Int): Unit =
    query.slop(slop)

  def multiMatch(q: String, mmType: MultiMatchQueryBuilder.Type): MultiMatchQueryBuilder =
    QueryBuilders.multiMatchQuery(q)
      .field(FullTextSearchAnalyzedFieldType.fieldName)
      .field(FullTextSearchRawFieldType.fieldName)
      .`type`(mmType)
}
