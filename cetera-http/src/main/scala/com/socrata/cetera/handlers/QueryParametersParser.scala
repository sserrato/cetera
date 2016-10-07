package com.socrata.cetera.handlers

import com.socrata.cetera.handlers.util._
import com.socrata.cetera.search.Sorts
import com.socrata.cetera.types._

// These are validated input parameters but aren't supposed to know anything about ES
case class ValidatedQueryParameters(
    searchParamSet: SearchParamSet,
    scoringParamset: ScoringParamSet,
    pagingParamSet: PagingParamSet,
    formatParamSet: FormatParamSet)

case class ValidatedUserQueryParameters(
    searchParamSet: UserSearchParamSet,
    pagingParamSet: PagingParamSet)

// NOTE: this is really a validation error, not a parse error
sealed trait ParseError { def message: String }

case class DatatypeError(override val message: String) extends ParseError

// Parses and validates
object QueryParametersParser { // scalastyle:ignore number.of.methods
  val filterDelimiter = "," // to be deprecated
  val limitLimit = 10000

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
  private val allowedSortOrders = Sorts.paramSortMap.keySet
  case class SortOrderString(value: String)
  implicit val sortOrderStringParamConverter = ParamConverter.filtered { (a: String) =>
    if (allowedSortOrders.contains(a)) Some(SortOrderString(a)) else None
  }

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

  // Used to support param=this,that and param[]=something&param[]=more
  // with the understanding that this produces values "this,that", "something" and "more"
  private def mergeArrayCommaParams(
      queryParameters: MultiQueryParams,
      key: String)
    : Option[Set[String]] = {
    val keys = Set(key, Params.arrayify(key))
    mergeOptionalSets(keys.map(key => queryParameters.get(key).map(_.toSet)))
  }

  val allowedFilterTypes = Datatypes.all.flatMap(d => Seq(d.plural, d.singular)).mkString(filterDelimiter)

  // This can stay case-sensitive because it is so specific
  def restrictParamFilterDatatype(datatype: String): Either[DatatypeError, Option[Set[String]]] = datatype match {
    case s: String if s.nonEmpty =>
      Datatype(s) match {
        case None => Left(DatatypeError(s"'${Params.filterDatatypes}' must be one of $allowedFilterTypes; got $s"))
        case Some(d) => Right(Some(d.names.toSet))
      }
    case _ => Right(None)
  }

  def restrictParamFilterDatatype(datatype: Option[String]): Either[DatatypeError, Option[Set[String]]] =
    datatype.map(restrictParamFilterDatatype).getOrElse(Right(None))

  // for extracting `example.com` from `boostDomains[example.com]`
  class FieldExtractor(val key: String) {
    def unapply(s: String): Option[String] =
      if (s.startsWith(key + "[") && s.endsWith("]")) Some(s.drop(key.length + 1).dropRight(1)) else None
  }

  object FloatExtractor {
    def unapply(s: String): Option[Float] = try { Some(s.toFloat) } catch { case _: NumberFormatException => None }
  }

  def prepareSortOrder(queryParameters: MultiQueryParams): Option[String] =
    queryParameters.typedFirst[SortOrderString](Params.sortOrder).map(validated(_).value)

  //////////////////
  // PARAM PREPARERS
  //
  // Prepare means parse and validate

  private def filterNonEmptySetParams(params: Option[Set[String]]): Option[Set[String]] =
    params.flatMap { ps: Set[String] =>
      ps.filter(_.nonEmpty) match {
        case filteredParams: Set[String] if filteredParams.nonEmpty => Some(filteredParams)
        case _ => None
      }
    }

  private def filterNonEmptyStringParams(params: Option[String]): Option[String] =
    params.getOrElse("") match {
      case x: String if x.nonEmpty => params
      case _ => None
    }

  def prepareLocale(queryParametesrs: MultiQueryParams): Option[String] =
    queryParametesrs.first(Params.locale).map(_.toLowerCase)

  def prepareSearchQuery(queryParameters: MultiQueryParams): QueryType =
    pickQuery(
      filterNonEmptyStringParams(queryParameters.get(Params.queryAdvanced).flatMap(_.headOption)),
      filterNonEmptyStringParams(queryParameters.get(Params.querySimple).flatMap(_.headOption))
    )

  // Still uses old-style comma-separation
  def prepareDomains(queryParameters: MultiQueryParams): Option[Set[String]] =
    filterNonEmptySetParams(queryParameters.first(Params.filterDomains).map(domain =>
      domain.toLowerCase.split(filterDelimiter).toSet
    ))

  // if query string includes search context use that; otherwise default to the http header X-Socrata-Host
  def prepareSearchContext(queryParameters: MultiQueryParams, extendedHost: Option[String]): Option[String] = {
    val contextSet = queryParameters.first(Params.context).map(_.toLowerCase)
    filterNonEmptyStringParams(Seq(contextSet, extendedHost).flatten.headOption)
  }

  def prepareCategories(queryParameters: MultiQueryParams): Option[Set[String]] =
    filterNonEmptySetParams(mergeArrayCommaParams(queryParameters, Params.filterCategories))

  def prepareTags(queryParameters: MultiQueryParams): Option[Set[String]] =
    filterNonEmptySetParams(mergeArrayCommaParams(queryParameters, Params.filterTags))

  def prepareDatatypes(queryParameters: MultiQueryParams): Either[DatatypeError, Option[Set[String]]] = {
    val csvParams = queryParameters.get(Params.filterDatatypes).map(_.flatMap(_.split(filterDelimiter))).map(_.toSet)
    val arrayParams = queryParameters.get(Params.arrayify(Params.filterDatatypes)).map(_.toSet)
    val mergedParams = mergeOptionalSets[String](Set(csvParams, arrayParams))

    mergedParams.map { params =>
      val parsedDatatypes = params.map(restrictParamFilterDatatype)

      val errors = parsedDatatypes.collect { case Left(err) => err }.headOption
      val datatypes = parsedDatatypes.collect { case Right(Some(ds)) => ds }.flatten

      (errors, datatypes) match {
        case (Some(es), _) => Left(es)
        case (_, ds)       => Right(Some(ds))
        case _             => Right(None)
      }
    }.getOrElse(Right(None))
  }

  def prepareUsers(queryParameters: MultiQueryParams): Option[String] =
    filterNonEmptyStringParams(queryParameters.first(Params.filterUser))

  def prepareSharedTo(queryParameters: MultiQueryParams): Option[String] =
    filterNonEmptyStringParams(queryParameters.first(Params.filterSharedTo))

  def prepareAttribution(queryParameters: MultiQueryParams): Option[String] =
    filterNonEmptyStringParams(queryParameters.first(Params.filterAttribution))

  def prepareParentDatasetId(queryParameters: MultiQueryParams): Option[String] =
    filterNonEmptyStringParams(queryParameters.first(Params.filterParentDatasetId))

  def prepareDomainMetadata(queryParameters: MultiQueryParams): Option[Set[(String, String)]] = {
    val queryParamsNonEmpty = queryParameters.filter { case (key, value) => key.nonEmpty && value.nonEmpty }
    // TODO: EN-1401 - don't just take head, allow for mulitple selections
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
  def prepareMinShouldMatch(queryParameters: MultiQueryParams): Option[String] = {
    val searchQuery = prepareSearchQuery(queryParameters)
    queryParameters.first(Params.minMatch).flatMap { p =>
      MinShouldMatch.fromParam(searchQuery, p)
    }
  }

  def prepareSlop(queryParameters: MultiQueryParams): Option[Int] =
    queryParameters.typedFirst[Int](Params.slop).map(validated)

  def prepareShowScore(queryParameters: MultiQueryParams): Boolean =
    queryParameters.contains(Params.showScore)

  def prepareShowVisiblity(queryParameters: MultiQueryParams): Boolean =
    queryParameters.contains(Params.showVisibility)

  def prepareOffset(queryParameters: MultiQueryParams): Int =
    validated(
      queryParameters.typedFirstOrElse(Params.scanOffset, NonNegativeInt(PagingParamSet.defaultPageOffset))
    ).value

  def prepareLimit(queryParameters: MultiQueryParams): Int =
    Math.min(
      limitLimit,
      validated(
        queryParameters.typedFirstOrElse(Params.scanLength, NonNegativeInt(PagingParamSet.defaultPageLength))
      ).value
    )

  def prepareShowHidden(queryParameters: MultiQueryParams): Boolean =
    queryParameters.contains(Params.showHidden)

  //  user search param preparers

  def prepareEmail(queryParameters: MultiQueryParams): Option[Set[String]] =
    filterNonEmptySetParams(mergeArrayCommaParams(queryParameters, Params.filterEmail))

  def prepareScreenName(queryParameters: MultiQueryParams): Option[Set[String]] =
    filterNonEmptySetParams(mergeArrayCommaParams(queryParameters, Params.filterScreenName))

  def prepareFlag(queryParameters: MultiQueryParams): Option[Set[String]] =
    filterNonEmptySetParams(mergeArrayCommaParams(queryParameters, Params.filterFlag))

  def prepareRole(queryParameters: MultiQueryParams): Option[Set[String]] =
    filterNonEmptySetParams(mergeArrayCommaParams(queryParameters, Params.filterRole))

  def prepareUserDomain(queryParameters: MultiQueryParams): Option[String] =
    filterNonEmptyStringParams(queryParameters.first(Params.filterDomain))

  def prepareUserQuery(queryParameters: MultiQueryParams): Option[String] =
    filterNonEmptyStringParams(queryParameters.first(Params.querySimple))

  // shared param preparers

  def prepareId(queryParameters: MultiQueryParams): Option[Set[String]] =
    filterNonEmptySetParams(mergeArrayCommaParams(queryParameters, Params.filterId))


  //////////////////

  ////////
  // APPLY
  //
  // NOTE: Watch out for case sensitivity in params
  // Some field values are stored internally in lowercase, others are not
  // Yes, the params parser now concerns itself with ES internals
  def apply(
      queryParameters: MultiQueryParams,
      extendedHost: Option[String])
    : Either[Seq[ParseError], ValidatedQueryParameters] = {

    // NOTE! We don't have to run most of these if just any of them fail validation
    // Add params to the match to provide helpful error messages
    prepareDatatypes(queryParameters) match {
      case Left(e)          => Left(Seq(e))
      case Right(datatypes) =>
        val searchParams = SearchParamSet(
          prepareSearchQuery(queryParameters),
          prepareDomains(queryParameters),
          prepareSearchContext(queryParameters, extendedHost),
          prepareDomainMetadata(queryParameters),
          prepareCategories(queryParameters),
          prepareTags(queryParameters),
          datatypes,
          prepareUsers(queryParameters),
          prepareSharedTo(queryParameters),
          prepareAttribution(queryParameters),
          prepareParentDatasetId(queryParameters),
          prepareId(queryParameters),
          prepareShowHidden(queryParameters)
        )
        val scoringParams = ScoringParamSet(
          prepareFieldBoosts(queryParameters),
          prepareDatatypeBoosts(queryParameters),
          prepareDomainBoosts(queryParameters),
          prepareMinShouldMatch(queryParameters),
          prepareSlop(queryParameters)
        )
        val pagingParams = PagingParamSet(
          prepareOffset(queryParameters),
          prepareLimit(queryParameters),
          prepareSortOrder(queryParameters)
        )
        val formatParams = FormatParamSet(
          prepareShowScore(queryParameters),
          prepareShowVisiblity(queryParameters),
          prepareLocale(queryParameters)
        )
        Right(ValidatedQueryParameters(searchParams, scoringParams, pagingParams, formatParams))
    }
  }

  def prepUserParams(queryParameters: MultiQueryParams): ValidatedUserQueryParameters = {
     val searchParams = UserSearchParamSet(
       prepareId(queryParameters),
       prepareEmail(queryParameters),
       prepareScreenName(queryParameters),
       prepareFlag(queryParameters),
       prepareRole(queryParameters),
       prepareUserDomain(queryParameters),
       prepareUserQuery(queryParameters)
     )
     val pagingParams = PagingParamSet(
       prepareOffset(queryParameters),
       prepareLimit(queryParameters)
     )
     ValidatedUserQueryParameters(searchParams, pagingParams)
  }
}

object Params {

  def arrayify(param: String): String = s"${param}[]"

  // catalog params
  val context = "search_context"
  val filterDomains = "domains"
  val filterCategories = "categories"
  val filterTags = "tags"
  val filterDatatypes = "only"
  val filterUser = "for_user"
  val filterAttribution = "attribution"
  val filterParentDatasetId = "derived_from"
  val filterSharedTo = "shared_to"

  val showHidden = "show_hidden"

  val queryAdvanced = "q_internal"
  val querySimple = "q"

  // whitepages params
  val filterId = "ids"
  val filterEmail = "emails"
  val filterScreenName = "screen_names"
  val filterFlag = "flags"
  val filterRole = "roles"
  val filterDomain = "domain"

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

  private val datatypeBoostParams = Datatypes.all.map(datatype => s"$boostParamPrefix${datatype.plural.capitalize}")

  private def typeNameFromBoostParam(boostParam: String): Option[String] =
    Some(boostParam).collect { case s: String if s.startsWith(boostParamPrefix) => s.stripPrefix(boostParamPrefix) }

  def datatypeBoostParam(param: String): Option[Datatype] =
    datatypeBoostParams.find(_ == param).flatMap { boostParam =>
      Datatype(typeNameFromBoostParam(boostParam.toLowerCase))
    }

  // field boosting parameters
  val boostColumns = boostParamPrefix + "Columns"
  val boostDescription = boostParamPrefix + "Desc"
  val boostTitle = boostParamPrefix + "Title"

  // e.g., boostDomains[example.com]=1.23&boostDomains[data.seattle.gov]=4.56
  val boostDomains = boostParamPrefix + "Domains"

  val functionScore = "function_score"
  val minMatch = "min_should_match"
  val slop = "slop"

  // result formatting parameters
  val locale = "locale"
  val showFeatureValues = "show_feature_vals" // TODO: revive feature values for relevance experimentation
  val showScore = "show_score"
  val showVisibility = "show_visibility"

  // sorting and pagination parameters
  val scanLength = "limit"
  val scanOffset = "offset"
  val sortOrder = "order"


  ///////////////////////
  // Explicit Param Lists
  ///////////////////////

  // HEY! when adding/removing parameters update `keys` or `mapKeys` as appropriate.
  // These are used to distinguish catalog keys from custom metadata fields

  // If your param is a simple key/value pair, add it here
  private val catalogKeys = Set(
    filterAttribution,
    context,
    filterDomains,
    filterCategories,
    filterTags,
    filterDatatypes,
    filterUser,
    filterParentDatasetId,
    filterId,
    locale,
    filterSharedTo,
    queryAdvanced,
    querySimple,
    boostColumns,
    boostDescription,
    boostTitle,
    functionScore,
    minMatch,
    slop,
    showFeatureValues,
    showScore,
    showVisibility,
    scanLength,
    scanOffset,
    sortOrder,
    showHidden
  ) ++ datatypeBoostParams.toSet

  // If your param is an array like tags[]=fun&tags[]=ice+cream, add it here
  private val catalogArrayKeys = Set(
    arrayify(filterCategories),
    arrayify(filterDatatypes),
    arrayify(filterTags)
  )

  // If your param is a hashmap like boostDomains[example.com]=1.23, add it here
  private val catalogMapKeys = Set(
    boostDomains
  )

  // For example: search_context, tags[], boostDomains[example.com]
  def isCatalogKey(key: String): Boolean = {
    catalogKeys.contains(key) ||
      catalogArrayKeys.contains(key) ||
      key.endsWith("]") && catalogMapKeys.contains(key.split('[').head)
  }

  // Remaining keys are treated as custom metadata fields and filtered on
  def remaining(qs: MultiQueryParams): MultiQueryParams = {
    qs.filterKeys { key => !isCatalogKey(key) }
  }
}
