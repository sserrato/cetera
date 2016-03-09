package com.socrata.cetera.search

import org.elasticsearch.index.query.{MultiMatchQueryBuilder, QueryBuilders}

object SundryBuilders {
  def applyMinMatchConstraint(query: MultiMatchQueryBuilder, constraint: String): Unit =
    query.minimumShouldMatch(constraint)

  def applySlopParam(query: MultiMatchQueryBuilder, slop: Int): Unit =
    query.slop(slop)

  def multiMatch(q: String, mmType: MultiMatchQueryBuilder.Type): MultiMatchQueryBuilder =
    QueryBuilders.multiMatchQuery(q)
      .field("fts_analyzed")
      .field("fts_raw")
      .`type`(mmType)
}
