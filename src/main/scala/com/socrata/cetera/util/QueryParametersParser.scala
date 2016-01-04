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
  datatypeBoosts: Map[Datatype, Float],
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

  def validated[T](x: Either[ParamConversionFailure, T]): T = x match {
    case Right(v) => v
    case Left(_) => throw new Exception("Parameter validation failure")
  }

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

  private def mergeOptionalSets[T](ss: Set[Option[Set[T]]]): Option[Set[T]] = {
    val cs = ss.foldLeft(Set.empty[T]) { (acc, ost) => acc ++ ost.getOrElse(Set.empty[T]) }
    if (cs.nonEmpty) Some(cs) else None
  }

  private def mergeParams(queryParameters: MultiQueryParams,
                          selectKeys: Set[String],
                          transform: String => String = identity): Option[Set[String]] =
    mergeOptionalSets(selectKeys.map(key => queryParameters.get(key).map(_.map(value => transform(value)).toSet)))

  val allowedTypes = Datatypes.all.flatMap(d => Seq(d.plural, d.singular)).mkString(",")

  // This can stay case-sensitive because it is so specific
  def restrictParamFilterType(only: Option[String]): Either[OnlyError, Option[Seq[String]]] =
    only.map { s =>
      Datatype(s) match {
        case None => Left(OnlyError(s"'only' must be one of $allowedTypes; got $s"))
        case Some(d) => Right(Some(d.names))
      }
    }.getOrElse(Right(None))

  def apply(req: HttpRequest): Either[Seq[ParseError], ValidatedQueryParameters] =
    apply(req.multiQueryParams)

  // Convert these params to lower case because of Elasticsearch filters
  // Yes, the params parser now concerns itself with ES internals
  def apply(queryParameters: MultiQueryParams): Either[Seq[ParseError], ValidatedQueryParameters] = {
    val query = pickQuery(
      queryParameters.get(Params.querySimple).flatMap(_.headOption),
      queryParameters.get(Params.queryAdvanced).flatMap(_.headOption)
    )

    val fieldBoosts = {
      val boostTitle = queryParameters.typedFirst[NonNegativeFloat](Params.boostTitle).map(validated(_).value)
      val boostDesc = queryParameters.typedFirst[NonNegativeFloat](Params.boostDescription).map(validated(_).value)
      val boostColumns = queryParameters.typedFirst[NonNegativeFloat](Params.boostColumns).map(validated(_).value)

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
        queryParameters.typedFirst[NonNegativeFloat](k).map(boost =>
          (datatype, validated(boost).value)))
    }

    restrictParamFilterType(queryParameters.first(Params.filterType)) match {
      case Right(o) =>
        Right(ValidatedQueryParameters(
          query,
          queryParameters.first(Params.filterDomains).map(_.toLowerCase.split(filterDelimiter).toSet),
          queryStringDomainMetadata(queryParameters),
          queryParameters.first(Params.context).map(_.toLowerCase),
          mergeParams(queryParameters, Set(Params.filterCategories, Params.filterCategoriesArray)),
          mergeParams(queryParameters, Set(Params.filterTags, Params.filterTagsArray), _.toLowerCase),
          o,
          fieldBoosts,
          datatypeBoosts,
          queryParameters.first(Params.minMatch).flatMap { p => MinShouldMatch.fromParam(query, p) },
          queryParameters.typedFirst[Int](Params.slop).map(validated), // Check for slop
          queryParameters.contains(Params.showScore), // just a flag
          validated(queryParameters.typedFirstOrElse(Params.scanOffset, NonNegativeInt(defaultPageOffset))).value,
          validated(queryParameters.typedFirstOrElse(Params.scanLength, NonNegativeInt(defaultPageLength))).value
        ))
      case Left(e) => Left(Seq(e))
    }
  }

  private def queryStringDomainMetadata(queryParameters: MultiQueryParams): Option[Set[(String, String)]] = {
    val ms = Params.remaining(queryParameters).mapValues(_.head).toSet
    if (ms.nonEmpty) Some(ms) else None
  }
}

object Params {
  val context = "search_context"
  val filterDomains = "domains"
  val filterCategories = "categories"
  val filterCategoriesArray = "categories[]"
  val filterTags = "tags"
  val filterTagsArray = "tags[]"
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

  def datatypeBoostParam(param: String): Option[Datatype] =
    datatypeBoostParams.find(_ == param).flatMap(boostParam =>
      Datatype(typeNameFromBoostParam(boostParam.toLowerCase)))

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
    filterCategoriesArray,
    filterTags,
    filterTagsArray,
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

  def remaining(qs: MultiQueryParams): MultiQueryParams = qs.filterKeys(key => !keys.contains(key))
}
