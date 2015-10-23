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
  def getScriptFunction(name: String): String =
    name match {
      case "views" => """1 + doc["page_views.page_views_total_log"].value"""
      case "score" => "_score"
      case _ => throw new Exception(s"Unrecognized ScriptScoreFunction $name")
    }
}
