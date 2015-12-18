package com.socrata.cetera.search

import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder
import org.elasticsearch.index.query.functionscore.script.ScriptScoreFunctionBuilder
import org.elasticsearch.index.query.{BoolQueryBuilder, MultiMatchQueryBuilder, QueryBuilders}
import org.elasticsearch.search.sort.{SortBuilder, SortBuilders, SortOrder}

import com.socrata.cetera.types.{DatatypeFieldType, DatatypeSimple}

object SundryBuilders {
  def addMinMatchConstraint(query: MultiMatchQueryBuilder, constraint: String): MultiMatchQueryBuilder =
    query.minimumShouldMatch(constraint)

  def addSlopParam(query: MultiMatchQueryBuilder, slop: Int): MultiMatchQueryBuilder = query.slop(slop)

  def addFunctionScores(scriptScoreFunctions: Set[ScriptScoreFunctionBuilder],
                        query: FunctionScoreQueryBuilder): FunctionScoreQueryBuilder = {
    scriptScoreFunctions.foreach { script => query.add(script) }

    // Take a product of scores and replace original score with product
    query.scoreMode("multiply").boostMode("replace")
  }

  def boostTypes(typeBoosts: Map[DatatypeSimple, Float]): BoolQueryBuilder = {
    typeBoosts.foldLeft(QueryBuilders.boolQuery()) { case (q, (datatype, boost)) =>
      q.should(QueryBuilders.termQuery(DatatypeFieldType.fieldName, datatype.singular).boost(boost))
    }
  }

  def multiMatch(q: String, mmType: MultiMatchQueryBuilder.Type): MultiMatchQueryBuilder =
    QueryBuilders.multiMatchQuery(q)
      .field("fts_analyzed")
      .field("fts_raw")
      .field("domain_cname")
      .`type`(mmType)

  val sortScoreDesc: SortBuilder = SortBuilders.scoreSort().order(SortOrder.DESC)
  def sortFieldAsc(field: String): SortBuilder = SortBuilders.fieldSort(field).order(SortOrder.ASC)
  def sortFieldDesc(field: String): SortBuilder = SortBuilders.fieldSort(field).order(SortOrder.DESC)
}
