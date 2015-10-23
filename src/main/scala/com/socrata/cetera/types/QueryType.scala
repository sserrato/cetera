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

case class ScriptScoreFunction(script: String, params: Map[String, AnyRef])

object ScriptScoreFunction {
  val rankingFunctionRe = """([a-zA-z_]+)\((-?[\d.]+)\s+(-?[\d.]+)\s+(-?[\d.]+)\)""".r

  def basicFnBody(field: String): String = s"""weight * ($field - mean) / std"""

  def getScriptFunction(name: String): String =
    name match {
      case "views" => """1 + doc["page_views.page_views_total_log"].value"""
      case "score" => basicFnBody("_score")
      case _ => throw new Exception(s"Unrecognized ScriptScoreFunction $name")
    }

  def fromParam(qt: QueryType, p: String): Option[ScriptScoreFunction] =
    qt match {
      case SimpleQuery(_) =>
        p match {
          case rankingFunctionRe(name, mean, std, weight) =>
            Some(ScriptScoreFunction(
                   getScriptFunction(name),
                   Map(
                     "mean" -> (mean.toDouble: java.lang.Double),
                     "std" -> (std.toDouble: java.lang.Double),
                     "weight" -> (weight.toDouble: java.lang.Double))))
          case _ => None
        }
      case _ => None
    }
}
