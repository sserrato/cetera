package com.socrata.cetera.search

import org.elasticsearch.index.query.functionscore.{FunctionScoreQueryBuilder, ScoreFunctionBuilders}
import org.elasticsearch.index.query.{BoolQueryBuilder, FilterBuilders, QueryBuilders}

import com.socrata.cetera._
import com.socrata.cetera.types.{Datatype, DatatypeFieldType, DomainCnameFieldType, ScriptScoreFunction}

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
          // TODO: refactor out has_parent filters
          FilterBuilders.hasParentFilter(esDomainType,
            FilterBuilders.termFilter(DomainCnameFieldType.rawFieldName, domain)
          ),
          ScoreFunctionBuilders.weightFactorFunction(weight)
        )
    }
  }
}
