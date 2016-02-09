package com.socrata.cetera.search

import org.elasticsearch.index.query.{MultiMatchQueryBuilder, QueryBuilders}

object SundryBuilders {
  def addMinMatchConstraint(query: MultiMatchQueryBuilder, constraint: String): MultiMatchQueryBuilder =
    query.minimumShouldMatch(constraint)

  def addSlopParam(query: MultiMatchQueryBuilder, slop: Int): MultiMatchQueryBuilder = query.slop(slop)

  def multiMatch(q: String, mmType: MultiMatchQueryBuilder.Type): MultiMatchQueryBuilder =
    QueryBuilders.multiMatchQuery(q)
      .field("fts_analyzed")
      .field("fts_raw")
      .field("domain_cname")
      .`type`(mmType)
}
