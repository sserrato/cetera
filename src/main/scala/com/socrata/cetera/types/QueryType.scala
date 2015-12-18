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
  private def script(script: String, lang: String = "expression"): ScriptScoreFunction =
    ScriptScoreFunction(s"""
      |"script": {
      |  "lang": "$lang",
      |  "inline": "$script"
      |}
    """.stripMargin)

  protected val VIEWS = script("1 + doc['page_views.page_views_total_log'].value")
  protected val SCORE = script("_score")

  def getScriptFunction(name: String): Option[ScriptScoreFunction] =
    name match {
      case "views" => Option(VIEWS)
      case "score" => Option(SCORE)
      case _ => None // log this?
    }
}
