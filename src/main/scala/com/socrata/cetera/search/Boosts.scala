package com.socrata.cetera.search

import org.elasticsearch.index.query.{BoolQueryBuilder, FilterBuilders, QueryBuilders}
import org.elasticsearch.index.query.functionscore.{FunctionScoreQueryBuilder, ScoreFunctionBuilders}

import com.socrata.cetera.types.{Datatype, DatatypeFieldType, DomainFieldType, ScriptScoreFunction}

object Boosts {
  def applyDatatypeBoosts(
      query: BoolQueryBuilder,
      datatypeBoosts
    : Map[Datatype, Float]): Unit = {

    datatypeBoosts.foreach {
      case (datatype, boost) =>
        query.should(
          QueryBuilders.termQuery(
            DatatypeFieldType.fieldName,
            datatype.singular
          ).boost(boost)
        )
    }
  }

  def applyScoreFunctions(
      query: FunctionScoreQueryBuilder,
      scriptScoreFunctions: Set[ScriptScoreFunction])
    : Unit = {

    scriptScoreFunctions.foreach { fn =>
      query.add(ScoreFunctionBuilders.scriptFunction(fn.script, "expression"))
    }
  }

  def applyDomainBoosts(
      query: FunctionScoreQueryBuilder,
      domainBoosts: Map[String, Float])
    : Unit = {

    domainBoosts.foreach {
      case (domain, weight) =>
        query.add(
          FilterBuilders.termFilter(DomainFieldType.rawFieldName, domain),
          ScoreFunctionBuilders.weightFactorFunction(weight)
        )
    }
  }
}
