package com.socrata.cetera.types

import scala.collection.JavaConverters._

import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders
import org.elasticsearch.index.query.functionscore.script.ScriptScoreFunctionBuilder
import org.elasticsearch.script.Script
import org.elasticsearch.script.ScriptService.ScriptType

sealed trait QueryType

case object NoQuery extends QueryType

case class SimpleQuery(query: String) extends QueryType

case class AdvancedQuery(query: String) extends QueryType

object MinShouldMatch {
  def fromParam(qt: QueryType, s: String): Option[String] =
    qt match {
      case SimpleQuery(_) => Option(s.trim)
      case _ => None
    }
}

object ScriptScoreFunction {
  private def fromScript(script: String, lang: String = "expression"): ScriptScoreFunctionBuilder =
    ScoreFunctionBuilders.scriptFunction(
      new Script(
        script,
        ScriptType.INLINE,
        lang,
        Map.empty[String,AnyRef].asJava
      ))

  protected val VIEWS = fromScript("1 + doc['page_views.page_views_total_log'].value")
  protected val SCORE = fromScript("_score")

  def fromName(name: String): Option[ScriptScoreFunctionBuilder] =
    name match {
      case "views" => Option(VIEWS)
      case "score" => Option(SCORE)
      case _ => None // log this?
    }
}
