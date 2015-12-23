package com.socrata.cetera.util

import com.socrata.http.server.HttpRequest

import com.socrata.cetera.types._

case class ValidatedQueryParameters(
  searchQuery: QueryType,
  domains: Option[Set[String]],
  domainMetadata: Option[Set[(String, String)]],
  searchContext: Option[String],
  categories: Option[Set[String]],
  tags: Option[Set[String]],
  only: Option[Seq[String]],
  fieldBoosts: Map[CeteraFieldType with Boostable, Float],
  datatypeBoosts: Map[DatatypeSimple, Float],
  minShouldMatch: Option[String],
  slop: Option[Int],
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
  val filterDelimiter: String = ","

  /////////////////////////////////////////////////////
  // code from rjmac likely to be added to socrata-http
  //
  sealed trait ParamConversionFailure
  case class InvalidValue(v: String) extends ParamConversionFailure

  trait ParamConverter[T] {
    def convertFrom(s: String): Either[ParamConversionFailure, T]
  }

  // monkeys have been here
  implicit class TypedQueryParams(queryParameters: Map[String,String]) {
    def queryParam[T: ParamConverter](name: String): Option[Either[ParamConversionFailure, T]] = {
      queryParameters
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
        catch { case e: Exception => Left(InvalidValue(s)) }
      }
    }

    implicit object FloatParam extends ParamConverter[Float] {
      override def convertFrom(s: String): Either[ParamConversionFailure, Float] = {
        try { Right(s.toFloat) }
        catch { case e: Exception => Left(InvalidValue(s)) }
      }
    }

    def filtered[A: ParamConverter, B](f: A => Option[B]): ParamConverter[B] = {
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
      case _ => NoQuery
    }

  val allowedTypes = Datatypes.all.flatMap(d => Seq(d.plural, d.singular)).mkString(",")

  // This can stay case-sensitive because it is so specific
  def restrictParamFilterType(only: Option[String]): Either[OnlyError, Option[Seq[String]]] =
    only.map { s =>
      DatatypeSimple(s) match {
        case None => Left(OnlyError(s"'only' must be one of $allowedTypes; got $s"))
        case Some(d) => Right(Some(d.names))
      }
    }.getOrElse(Right(None))

  def apply(req: HttpRequest): Either[Seq[ParseError], ValidatedQueryParameters] =
    apply(req.queryParameters)

  // Convert these params to lower case because of Elasticsearch filters
  // Yes, the params parser now concerns itself with ES internals
  def apply(queryParameters: Map[String,String]): Either[Seq[ParseError], ValidatedQueryParameters] = {
    val query = pickQuery(queryParameters.get(Params.querySimple), queryParameters.get(Params.queryAdvanced))

    val fieldBoosts = {
      val boostTitle = queryParameters.queryParam[NonNegativeFloat](Params.boostTitle).map(validated(_).value)
      val boostDesc = queryParameters.queryParam[NonNegativeFloat](Params.boostDescription).map(validated(_).value)
      val boostColumns = queryParameters.queryParam[NonNegativeFloat](Params.boostColumns).map(validated(_).value)

      Map(
        TitleFieldType -> boostTitle,
        DescriptionFieldType -> boostDesc,
        ColumnNameFieldType -> boostColumns,
        ColumnDescriptionFieldType -> boostColumns,
        ColumnFieldNameFieldType -> boostColumns
      )
        .collect { case (fieldType, Some(weight)) => (fieldType, weight) }
        .toMap[CeteraFieldType with Boostable, Float]
    }

    val datatypeBoosts = queryParameters.flatMap {
      case (k, _) => Params.datatypeBoostParam(k).flatMap(datatype =>
        queryParameters.queryParam[NonNegativeFloat](k).map(boost =>
          (datatype, validated(boost).value)))
    }

    restrictParamFilterType(queryParameters.get(Params.filterType)) match {
      case Right(o) =>
        Right(ValidatedQueryParameters(
          query,
          queryParameters.get(Params.filterDomains).map(_.toLowerCase.split(filterDelimiter).toSet),
          Option(queryStringDomainMetadata(queryParameters)),
          queryParameters.get(Params.context).map(_.toLowerCase),
          queryParameters.get(Params.filterCategories).map(_.split(filterDelimiter).toSet),
          queryParameters.get(Params.filterTags).map(_.toLowerCase.split(filterDelimiter).toSet),
          o,
          fieldBoosts,
          datatypeBoosts,
          queryParameters.get(Params.minMatch).flatMap { case p: String => MinShouldMatch.fromParam(query, p) },
          queryParameters.queryParam[Int](Params.slop).map(validated), // Check for slop
          queryParameters.contains(Params.showScore), // just a flag
          validated(queryParameters.queryParamOrElse(Params.scanOffset, NonNegativeInt(defaultPageOffset))).value,
          validated(queryParameters.queryParamOrElse(Params.scanLength, NonNegativeInt(defaultPageLength))).value
        ))
      case Left(e) => Left(Seq(e))
    }
  }

  private def queryStringDomainMetadata(queryParameters: Map[String,String]): Set[(String, String)] =
    Params.remaining(queryParameters).toSet
}

object Params {
  val context = "search_context"
  val filterDomains = "domains"
  val filterCategories = "categories"
  val filterTags = "tags"
  val filterType = "only"

  val queryAdvanced = "q_internal"
  val querySimple = "q"

  // We allow catalog datatypes to be boosted using the boost{typename}={factor}
  // (eg. boostDatasets=10.0) syntax. To avoid redundancy, we get the available
  // types by way of the com.socrata.cetera.types.Datatypes.all method.
  // See com.socrata.cetera.types.Datatypes for the type definitions.
  //
  // Available datatype boosting parameters:
  //   boostCalendars
  //   boostDatalenses
  //   boostDatasets
  //   boostFiles
  //   boostFilters
  //   boostForms
  //   boostPulses
  //   boostStories
  //   boostLinks
  //   boostCharts
  //   boostMaps

  private val boostParamPrefix = "boost"

  private val datatypeBoostParams = Datatypes.all.map(
    datatype => s"$boostParamPrefix${datatype.plural.capitalize}")

  private def typeNameFromBoostParam(boostParam: String): Option[String] =
    if (boostParam.startsWith(boostParamPrefix)) {
      Option(boostParam.stripPrefix(boostParamPrefix))
    } else {
      None
    }

  def datatypeBoostParam(param: String): Option[DatatypeSimple] =
    datatypeBoostParams.find(_ == param).flatMap(boostParam =>
      DatatypeSimple(typeNameFromBoostParam(boostParam.toLowerCase)))

  // field boosting parameters
  val boostColumns = "boostColumns"
  val boostDescription = "boostDesc"
  val boostTitle = "boostTitle"

  val minMatch = "min_should_match"
  val slop = "slop"
  val showFeatureValues = "show_feature_vals"
  val showScore = "show_score"
  val functionScore = "function_score"

  val scanLength = "limit"
  val scanOffset = "offset"

  // HEY! when adding/removing parameters update this list.
  private val keys = List(
    context,
    filterDomains,
    filterCategories,
    filterTags,
    filterType,
    queryAdvanced,
    querySimple,
    boostColumns,
    boostDescription,
    boostTitle,
    minMatch,
    slop,
    showFeatureValues,
    showScore,
    functionScore,
    scanLength,
    scanOffset
  ) ++ datatypeBoostParams

  def remaining(qs: Map[String, String]): Map[String, String] =
    qs.filterKeys(key => !(keys.contains(key)))
}
