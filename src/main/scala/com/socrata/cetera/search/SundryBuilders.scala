package com.socrata.cetera.search

import org.elasticsearch.index.query.{BoolQueryBuilder, QueryBuilders, MultiMatchQueryBuilder}
import org.elasticsearch.index.query.functionscore.{ScoreFunctionBuilders, FunctionScoreQueryBuilder}
import org.elasticsearch.search.sort.{SortOrder, SortBuilders, SortBuilder}

import com.socrata.cetera.types.{ScriptScoreFunction, DatatypeFieldType, DatatypeSimple}

object SundryBuilders {
  def addMinMatchConstraint(query: MultiMatchQueryBuilder, constraint: String): MultiMatchQueryBuilder =
    query.minimumShouldMatch(constraint)

  def addSlopParam(query: MultiMatchQueryBuilder, slop: Int): MultiMatchQueryBuilder = query.slop(slop)

  def addFunctionScores(scriptScoreFunctions:Set[ScriptScoreFunction],
                        query: FunctionScoreQueryBuilder): FunctionScoreQueryBuilder = {
    scriptScoreFunctions.foreach { fn =>
      query.add(ScoreFunctionBuilders.scriptFunction(fn.script, "expression"))
    }

    // Take a product of scores and replace original score with product
    query.scoreMode("multiply").boostMode("replace")
  }

  def boostTypes(typeBoosts: Map[DatatypeSimple, Float]): BoolQueryBuilder = {
    typeBoosts.foldLeft(QueryBuilders.boolQuery()) { case (q, (datatype, boost)) =>
      typeBoosts.foldLeft(QueryBuilders.boolQuery()) { case (q, (datatype, boost)) =>
        q.should(QueryBuilders.termQuery(DatatypeFieldType.fieldName, datatype.singular).boost(boost))
      }
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
