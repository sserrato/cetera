package com.socrata.cetera.search

import org.elasticsearch.index.query.{MultiMatchQueryBuilder, QueryBuilders}
import org.elasticsearch.index.query.functionscore.{FunctionScoreQueryBuilder, ScoreFunctionBuilders}

import com.socrata.cetera.types.ScriptScoreFunction

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

  def multiMatch(q: String, mmType: MultiMatchQueryBuilder.Type): MultiMatchQueryBuilder =
    QueryBuilders.multiMatchQuery(q)
      .field("fts_analyzed")
      .field("fts_raw")
      .field("domain_cname")
      .`type`(mmType)
}
