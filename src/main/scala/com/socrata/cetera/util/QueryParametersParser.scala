package com.socrata.cetera.util

import com.socrata.http.server.HttpRequest

import com.socrata.cetera.search.Sorts
import com.socrata.cetera.types._

// These are validated input parameters but aren't supposed to know anything about ES
case class ValidatedQueryParameters(
    searchQuery: QueryType,
    domains: Set[String],
    domainMetadata: Option[Set[(String, String)]],
    searchContext: Option[String],
    categories: Option[Set[String]],
    tags: Option[Set[String]],
    only: Option[Seq[String]],
    fieldBoosts: Map[CeteraFieldType with Boostable, Float],
    datatypeBoosts: Map[Datatype, Float],
    domainBoosts: Map[String, Float],
    minShouldMatch: Option[String],
    slop: Option[Int],
    showScore: Boolean,
    offset: Int,
    limit: Int,
    sortOrder: Option[String]
)

// NOTE: this is really a validation error, not a parse error
sealed trait ParseError { def message: String }

case class LimitError(override val message: String) extends ParseError
case class OffsetError(override val message: String) extends ParseError
case class OnlyError(override val message: String) extends ParseError
case class SortOrderError(override val message: String) extends ParseError

// Parses and validates
object QueryParametersParser {
  val defaultPageOffset: Int = 0
  val defaultPageLength: Int = 100
  val filterDelimiter: String = "," // to be deprecated

  def validated[T](x: Either[ParamConversionFailure, T]): T = x match {
    case Right(v) => v
    case Left(_) => throw new Exception("Parameter validation failure")
  }

  // for offset and limit
  case class NonNegativeInt(value: Int)
  implicit val nonNegativeIntParamConverter = ParamConverter.filtered { (a: Int) =>
    if (a >= 0) Some(NonNegativeInt(a)) else None
  }

  // for boosts
  case class NonNegativeFloat(value: Float)
  implicit val nonNegativeFloatParamConverter = ParamConverter.filtered { (a: Float) =>
    if (a >= 0.0f) Some(NonNegativeFloat(a)) else None
  }

  // for sort order
  case class SortOrderString(value: String)
  implicit val sortOrderStringParamConverter = ParamConverter.filtered { (a: String) =>
    if (allowedSortOrders.contains(a)) Some(SortOrderString(a)) else None
  }

  private val allowedSortOrders = Sorts.paramSortMap.keySet

  // If both are specified, prefer the advanced query over the search query
  private def pickQuery(advanced: Option[String], simple: Option[String]): QueryType =
    (advanced, simple) match {
      case (Some(aq), _) => AdvancedQuery(aq)
      case (_, Some(sq)) => SimpleQuery(sq)
      case _ => NoQuery
    }

  private def mergeOptionalSets[T](ss: Set[Option[Set[T]]]): Option[Set[T]] = {
    val cs = ss.flatMap(_.getOrElse(Set.empty[T]))
    if (cs.nonEmpty) Some(cs) else None
  }

  // Used to support param=this,that and param[]something&param[]=more
  private def mergeParams(queryParameters: MultiQueryParams,
                          selectKeys: Set[String],
                          transform: String => String = identity): Option[Set[String]] =
    mergeOptionalSets(selectKeys.map(key => queryParameters.get(key).map(_.map(value => transform(value)).toSet)))

  val allowedFilterTypes = Datatypes.all.flatMap(d => Seq(d.plural, d.singular)).mkString(",")

  // This can stay case-sensitive because it is so specific
  def restrictParamFilterType(only: Option[String]): Either[OnlyError, Option[Seq[String]]] =
    only.map { s =>
      Datatype(s) match {
        case None => Left(OnlyError(s"'only' must be one of $allowedFilterTypes; got $s"))
        case Some(d) => Right(Some(d.names))
      }
    }.getOrElse(Right(None))

  // for extracting `example.com` from `boostDomains[example.com]`
  class FieldExtractor(val key: String) {
    def unapply(s: String): Option[String] =
      if (s.startsWith(key + "[") && s.endsWith("]")) { Some(s.drop(key.length + 1).dropRight(1)) }
      else { None }
  }

  object FloatExtractor {
    def unapply(s: String): Option[Float] =
      try { Some(s.toFloat) }
      catch { case _: NumberFormatException => None }
  }

  def prepareSortOrder(queryParameters: MultiQueryParams): Option[String] = {
    queryParameters.typedFirst[SortOrderString](Params.sortOrder).map(validated(_).value)
  }

  def apply(req: HttpRequest): Either[Seq[ParseError], ValidatedQueryParameters] =
    apply(req.multiQueryParams)


  //////////////////
  // PARAM PREPARERS
  //
  // Prepare means parse and validate

  def prepareSearchQuery(queryParameters: MultiQueryParams) = {
    pickQuery(
      queryParameters.get(Params.queryAdvanced).flatMap(_.headOption),
      queryParameters.get(Params.querySimple).flatMap(_.headOption)
    )
  }

  def prepareDomains(queryParameters: MultiQueryParams): Set[String] = {
    queryParameters.first(Params.filterDomains) match {
      case Some(ds) => ds.toLowerCase.split(filterDelimiter).toSet
      case None => Set.empty[String]
    }
  }

  def prepareSearchContext(queryParameters: MultiQueryParams) = {
    queryParameters.first(Params.context).map(_.toLowerCase)
  }

  def prepareCategories(queryParameters: MultiQueryParams) = {
    mergeParams(queryParameters, Set(Params.filterCategories, Params.filterCategoriesArray))
  }

  def prepareTags(queryParameters: MultiQueryParams) = {
    mergeParams(queryParameters, Set(Params.filterTags, Params.filterTagsArray))
  }

  def prepareOnly(queryParameters: MultiQueryParams) = {
    restrictParamFilterType(queryParameters.first(Params.filterType))
  }

  def prepareDomainMetadata(queryParameters: MultiQueryParams): Option[Set[(String, String)]] = {
    val queryParamsNonEmpty = queryParameters.filter { case (key, value) => key.nonEmpty && value.nonEmpty }
    val ms = Params.remaining(queryParamsNonEmpty).mapValues(_.head).toSet
    if (ms.nonEmpty) Some(ms) else None
  }

  def prepareFieldBoosts(queryParameters: MultiQueryParams): Map[CeteraFieldType with Boostable, Float] = {
    val boostTitle = queryParameters.typedFirst[NonNegativeFloat](Params.boostTitle).map(validated(_).value)
    val boostDesc = queryParameters.typedFirst[NonNegativeFloat](Params.boostDescription).map(validated(_).value)
    val boostColumns = queryParameters.typedFirst[NonNegativeFloat](Params.boostColumns).map(validated(_).value)

    val fieldBoosts = Map(
      TitleFieldType -> boostTitle,
      DescriptionFieldType -> boostDesc,

      ColumnNameFieldType -> boostColumns,
      ColumnDescriptionFieldType -> boostColumns,
      ColumnFieldNameFieldType -> boostColumns
    )

    fieldBoosts
      .collect { case (fieldType, Some(weight)) => (fieldType, weight) }
      .toMap[CeteraFieldType with Boostable, Float]
  }

  def prepareDatatypeBoosts(queryParameters: MultiQueryParams): Map[Datatype, Float] = {
    queryParameters.flatMap {
      case (k, _) => Params.datatypeBoostParam(k).flatMap(datatype =>
        queryParameters.typedFirst[NonNegativeFloat](k).map(boost =>
          (datatype, validated(boost).value)))
    }
  }

  def prepareDomainBoosts(queryParameters: MultiQueryParams): Map[String, Float] = {
    val domainExtractor = new FieldExtractor(Params.boostDomains)
    queryParameters.collect { case (domainExtractor(field), Seq(FloatExtractor(weight))) =>
      field -> weight
    }
  }

  // Yes we compute searchQuery twice because this is a smell
  def prepareMinShouldMatch(queryParameters: MultiQueryParams) = {
    val searchQuery = prepareSearchQuery(queryParameters)
    queryParameters.first(Params.minMatch).flatMap { p =>
      MinShouldMatch.fromParam(searchQuery, p)
    }
  }

  def prepareSlop(queryParameters: MultiQueryParams) = {
    queryParameters.typedFirst[Int](Params.slop).map(validated)
  }

  def prepareShowScore(queryParameters: MultiQueryParams) = {
    queryParameters.contains(Params.showScore)
  }

  def prepareOffset(queryParameters: MultiQueryParams) = {
    validated(
      queryParameters.typedFirstOrElse(Params.scanOffset, NonNegativeInt(defaultPageOffset))
    ).value
  }

  def prepareLimit(queryParameters: MultiQueryParams) = {
    validated(
      queryParameters.typedFirstOrElse(Params.scanLength, NonNegativeInt(defaultPageLength))
    ).value
  }

  //
  //////////////////

  ////////
  // APPLY
  //
  // NOTE: Watch out for case sensitivity in params
  // Some field values are stored internally in lowercase, others are not
  // Yes, the params parser now concerns itself with ES internals
  def apply(queryParameters: MultiQueryParams): Either[Seq[ParseError], ValidatedQueryParameters] = {

    // NOTE! We don't have to run most of these if just any of them fail validation

    val searchQuery = prepareSearchQuery(queryParameters)
    val domains = prepareDomains(queryParameters)
    val domainMetadata = prepareDomainMetadata(queryParameters)
    val searchContext = prepareSearchContext(queryParameters)
    val categories = prepareCategories(queryParameters)
    val tags = prepareTags(queryParameters)

    val only = prepareOnly(queryParameters)

    val fieldBoosts = prepareFieldBoosts(queryParameters)
    val datatypeBoosts = prepareDatatypeBoosts(queryParameters)
    val domainBoosts = prepareDomainBoosts(queryParameters)
    val minShouldMatch = prepareMinShouldMatch(queryParameters)

    val slop = prepareSlop(queryParameters)
    val showScore = prepareShowScore(queryParameters)

    val offset = prepareOffset(queryParameters)
    val limit = prepareLimit(queryParameters)

    val sortOrder = prepareSortOrder(queryParameters)

    // TODO reorder semantically (searchContext before domainMetadata)
    // Add params to the match to provide helpful error messages
    only match {
      case Right(o) =>
        Right(
          ValidatedQueryParameters(
          searchQuery,
          domains,
          domainMetadata,
          searchContext,
          categories,
          tags,
          o,
          fieldBoosts,
          datatypeBoosts,
          domainBoosts,
          minShouldMatch,
          slop,
          showScore,
          offset,
          limit,
          sortOrder
          )
        )
      case Left(e) => Left(Seq(e))
    }
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
  val boostColumns = boostParamPrefix + "Columns"
  val boostDescription = boostParamPrefix + "Desc"
  val boostTitle = boostParamPrefix + "Title"

  // e.g., boostDomains[example.com]=1.23&boostDomains[data.seattle.gov]=4.56
  val boostDomains = boostParamPrefix + "Domains"

  val minMatch = "min_should_match"
  val slop = "slop"
  val showFeatureValues = "show_feature_vals"
  val showScore = "show_score"
  val functionScore = "function_score"

  val scanLength = "limit"
  val scanOffset = "offset"
  val sortOrder = "order"


  ///////////////////////
  // Explicit Param Lists
  ///////////////////////

  // HEY! when adding/removing parameters update `keys` or `mapKeys` as appropriate.
  // These are used to distinguish catalog keys from custom metadata fields

  // If your param is a simple key/value pair, add it here
  private val stringKeys = Set(
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
    scanOffset,
    sortOrder
  ) ++ datatypeBoostParams.toSet

  // If your param is an array like tags[]=fun&tags[]=ice+cream
  private val arrayKeys = Set(
    filterCategoriesArray,
    filterTagsArray
  )

  // If your param is a hashmap like boostDomains[example.com]=1.23, add it here
  private val mapKeys = Set(
    boostDomains
  )

  // For example: search_context, tags[], boostDomains[example.com]
  def isCatalogKey(key: String): Boolean = {
    stringKeys.contains(key) ||
      arrayKeys.contains(key) ||
      key.endsWith("]") && mapKeys.contains(key.split('[').head)
  }

  // Remaining keys are treated as custom metadata fields and filtered on
  def remaining(qs: MultiQueryParams): MultiQueryParams = {
    qs.filterKeys { key => !isCatalogKey(key) }
  }
}
