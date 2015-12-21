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

  def fromName(name: String, lang: String = "expression"): Option[ScriptScoreFunctionBuilder] =
    Option(
      ScoreFunctionBuilders.scriptFunction(
        new Script(
          name,
          ScriptType.INDEXED,
          lang,
          Map.empty[String,AnyRef].asJava
        )
      )
    )
}
