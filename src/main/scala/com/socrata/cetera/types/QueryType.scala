package com.socrata.cetera.types

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

case class ScriptScoreFunction(script: String)

object ScriptScoreFunction {
  def getScriptFunction(name: String): Option[ScriptScoreFunction] =
    name match {
      case "views" => Option(ScriptScoreFunction("""1 + doc["page_views.page_views_total_log"].value"""))
      case "score" => Option(ScriptScoreFunction("_score"))
      case _ => None // log this?
    }
}
