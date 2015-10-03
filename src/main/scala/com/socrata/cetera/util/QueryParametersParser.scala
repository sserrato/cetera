package com.socrata.cetera.util

import com.socrata.cetera.types._
import com.socrata.http.server.HttpRequest

case class ValidatedQueryParameters(
  searchQuery: QueryType,
  domains: Option[Set[String]],
  searchContext: Option[String],
  categories: Option[Set[String]],
  tags: Option[Set[String]],
  only: Option[String],
  boosts: Map[CeteraFieldType with Boostable, Float],
  minShouldMatch: Option[String],
  slop: Option[Int],
  functionScores: List[ScriptScoreFunction],
  showFeatureVals: Boolean,
  showScore: Boolean,
  offset: Int,
  limit: Int
)

sealed trait ParseError { def message: String }

case class OnlyError(override val message: String) extends ParseError
case class LimitError(override val message: String) extends ParseError

// Parses and validates
object QueryParametersParser {
  val defaultPageOffset: Int = 0
  val defaultPageLength: Int = 100

  /////////////////////////////////////////////////////
  // code from rjmac likely to be added to socrata-http
  //
  sealed trait ParamConversionFailure
  case class InvalidValue(v: String) extends ParamConversionFailure

  trait ParamConverter[T] {
    def convertFrom(s: String): Either[ParamConversionFailure, T]
  }

  // monkeys have been here
  implicit class TypedQueryParams(req: HttpRequest) {
    def queryParam[T: ParamConverter](name: String): Option[Either[ParamConversionFailure, T]] = {
      req.queryParameters
        .get(name)
        .map(implicitly[ParamConverter[T]].convertFrom)
    }

    def queryParamOrElse[T: ParamConverter](name: String, default: T): Either[ParamConversionFailure, T] = {
      queryParam[T](name).getOrElse(Right(default))
    }
  }

  def validated[T](x: Either[ParamConversionFailure, T]): T = x match {
    case Right(v) => v
    case Left(_) => throw new Exception("Parameter validation failure")
  }

  object ParamConverter {
    implicit object IntParam extends ParamConverter[Int] {
      override def convertFrom(s: String): Either[ParamConversionFailure, Int] = {
        try { Right(s.toInt) }
        catch { case e : Exception => Left(InvalidValue(s)) }
      }
    }

    implicit object FloatParam extends ParamConverter[Float] {
      override def convertFrom(s: String): Either[ParamConversionFailure, Float] = {
        try { Right(s.toFloat) }
        catch { case e : Exception => Left(InvalidValue(s)) }
      }
    }

    def filtered[A : ParamConverter, B](f: A => Option[B]): ParamConverter[B] = {
      new ParamConverter[B] {
        def convertFrom(s: String): Either[ParamConversionFailure, B] = {
          implicitly[ParamConverter[A]].convertFrom(s) match {
            case Right(a) =>
              f(a) match {
                case Some(b) => Right(b)
                case None => Left(InvalidValue(s))
              }
            case Left(e) => Left(e)
          }
        }
      }
    }
  }
  //
  /////////////////////////////////////////////////////


  // for offset and limit
  case class NonNegativeInt(value: Int)
  implicit val nonNegativeIntParamConverter = ParamConverter.filtered { (a: Int) =>
    if (a >= 0) Some(NonNegativeInt(a)) else None
  }

  // for boost
  case class NonNegativeFloat(value: Float)
  implicit val nonNegativeFloatParamConverter = ParamConverter.filtered { (a: Float) =>
    if (a >= 0.0f) Some(NonNegativeFloat(a)) else None
  }

  // If both are specified, prefer the advanced query over the search query
  private def pickQuery(simple: Option[String], advanced: Option[String]): QueryType =
    (simple, advanced) match {
      case (_, Some(aq)) => AdvancedQuery(aq)
      case (Some(sq), _) => SimpleQuery(sq)
      case _             => NoQuery
    }

  // Whether to return score in metadata
  private def showScore(query: QueryType, req: HttpRequest) = query match {
    case NoQuery => false
    case _       => req.queryParameters.contains("show_score")
  }

  // Check for function score functions
  private def scriptScoreFunctions(req: HttpRequest, query: QueryType): List[ScriptScoreFunction] =
    req.queryParameters.get("function_score").map(
      _.split(',').toList.flatMap {
        param => ScriptScoreFunction.fromParam(query, param)
      }).getOrElse(List.empty[ScriptScoreFunction])

  // This can stay case-sensitive because it is so specific
  private def restrictQueryParameters(req: HttpRequest): Either[OnlyError,Option[String]] =
    req.queryParameters.get("only") match {
      case None => Right(None)
      case Some("datasets") => Right(Some("dataset"))
      case Some("files") => Right(Some("file"))
      case Some("external") => Right(Some("href"))
      case Some("maps") => Right(Some("map"))
      case Some(invalid) => Left(OnlyError(s"'only' must be one of {datasets, files, external, maps}, got $invalid"))
    }

  // Convert these params to lower case because of Elasticsearch filters
  // Yes, the params parser now concerns itself with ES internals
  def apply(req: HttpRequest): Either[Seq[ParseError], ValidatedQueryParameters] = {
    val query = pickQuery(req.queryParameters.get("q"), req.queryParameters.get("q_internal"))
    val functionScores = scriptScoreFunctions(req, query)

    val boosts = {
      val boostTitle   = req.queryParam[NonNegativeFloat]("boostTitle").map(validated(_).value)
      val boostDesc    = req.queryParam[NonNegativeFloat]("boostDesc").map(validated(_).value)
      val boostColumns = req.queryParam[NonNegativeFloat]("boostColumns").map(validated(_).value)

      Map(TitleFieldType -> boostTitle,
          DescriptionFieldType -> boostDesc,
          ColumnNameFieldType -> boostColumns,
          ColumnDescriptionFieldType -> boostColumns,
          ColumnFieldNameFieldType -> boostColumns)
        .collect { case (fieldType, Some(weight)) => (fieldType, weight) }
        .toMap[CeteraFieldType with Boostable, Float]
    }

    restrictQueryParameters(req) match {
      case Right(o) =>
        Right(ValidatedQueryParameters(
          query,
          req.queryParameters.get("domains").map(_.toLowerCase.split(",").toSet),
          req.queryParameter("search_context").map(_.toLowerCase),
          req.queryParameters.get("categories").map(_.toLowerCase.split(",").toSet),
          req.queryParameters.get("tags").map(_.toLowerCase.split(",").toSet),
          o,
          boosts,
          req.queryParameters.get("min_should_match").flatMap { case p: String => MinShouldMatch.fromParam(query, p) },
          req.queryParam[Int]("slop").map(validated), // Check for slop
          functionScores,
          functionScores.nonEmpty && req.queryParameters.contains("show_feature_vals"),
          showScore(query, req),
          validated(req.queryParamOrElse("offset", NonNegativeInt(defaultPageOffset))).value,
          validated(req.queryParamOrElse("limit", NonNegativeInt(defaultPageLength))).value
        ))
      case Left(e) => Left(Seq(e))
    }
  }
}
