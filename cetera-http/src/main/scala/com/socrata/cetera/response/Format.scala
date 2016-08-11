package com.socrata.cetera.response

import com.rojoma.json.v3.ast.{JNumber, _}
import com.rojoma.json.v3.codec.DecodeError
import com.rojoma.json.v3.io.JsonReader
import com.rojoma.json.v3.jpath.JPath
import org.elasticsearch.action.search.SearchResponse
import org.slf4j.LoggerFactory

import com.socrata.cetera._
import com.socrata.cetera.handlers.FormatParamSet
import com.socrata.cetera.types._

object Format {
  lazy val logger = LoggerFactory.getLogger(Format.getClass)
  val UrlSegmentLengthLimit = 50

  val visibleToAnonKey = "visible_to_anonymous"

  def hyphenize(text: String): String = Option(text) match {
    case Some(s) if s.nonEmpty => s.replaceAll("[^\\p{L}\\p{N}_]+", "-").take(UrlSegmentLengthLimit)
    case _ => "-"
  }

  def links(
      cname: String,
      locale: Option[String],
      datatype: Option[Datatype],
      viewtype: Option[String],
      datasetId: String,
      datasetCategory: Option[String],
      datasetName: String,
      previewImageId: Option[String]): Map[String, JString] = {

    val cnameWithLocale = locale.foldLeft(cname){ (path, locale) => s"$path/$locale" }

    val perma = (datatype, viewtype) match {
      case (Some(TypeStories), _)             => s"stories/s"
      case (Some(TypeDatalenses), _)          => s"view"
      case (_, Some(TypeDatalenses.singular)) => s"view"
      case _                                  => s"d"
    }

    val urlSegmentLengthLimit = 50
    val pretty = datatype match {
      // TODO: maybe someday stories will allow pretty seo links
      // stories don't have a viewtype today, but who knows...
      case Some(TypeStories) => perma
      case _ =>
        val category = datasetCategory.filter(s => s.nonEmpty).getOrElse(TypeDatasets.singular)
        s"${hyphenize(category)}/${hyphenize(datasetName)}"
    }

    val previewImageUrl = previewImageId.map(id => JString(s"https://$cname/views/$datasetId/files/$id"))
    if (previewImageId.isEmpty) logger.info(s"Missing previewImageId field for document $datasetId")

    Map(
      "permalink" -> JString(s"https://$cnameWithLocale/$perma/$datasetId"),
      "link" -> JString(s"https://$cnameWithLocale/$pretty/$datasetId")
    ) ++ previewImageUrl.map(url => ("previewImageUrl", url))
  }

  // TODO: rammy to rename customer_blah to domain_blah
  def domainCategory(j: JValue): Option[JValue] = j.dyn.customer_category.? match {
    case Left(e) => None
    case Right(jv) => Some(jv)
  }

  def domainCategoryString(j: JValue): Option[String] =
    domainCategory(j).flatMap {
      case JString(s) => Option(s)
      case _ => None
    }

  def domainTags(j: JValue): Option[JValue] = j.dyn.customer_tags.? match {
    case Left(e) => None
    case Right(jv) => Some(jv)
  }

  def domainMetadata(j: JValue): Option[JValue] = j.dyn.customer_metadata_flattened.? match {
    case Left(e) => None
    case Right(jv) => Some(jv)
  }

  def categories(j: JValue): Seq[JValue] =
    new JPath(j).down("animl_annotations").down("categories").*.down("name").finish.distinct.toList

  def tags(j: JValue): Seq[JValue] =
    new JPath(j).down("animl_annotations").down("tags").*.down("name").finish.distinct.toList

  def cname(domainCnames: Map[Int,String], j: JValue): String = {
    domainId(j).flatMap { i =>
      domainCnames.get(i)
    }.getOrElse("") // if no domain was found, default to blank string
  }

  private def extractJString(decoded: Either[DecodeError, JValue]): Option[String] =
    decoded.fold(_ => None, {
      case JString(s) => Option(s)
      case _ => None
    })

  private def extractJBoolean(decoded: Either[DecodeError, JValue]): Option[Boolean] =
    decoded.fold(_ => None, {
      case JBoolean(b) => Option(b)
      case _ => None
    })

  def datatype(j: JValue): Option[Datatype] =
    extractJString(j.dyn.datatype.?).flatMap(s => Datatype(s))

  def viewtype(j: JValue): Option[String] = extractJString(j.dyn.viewtype.?)

  def datasetId(j: JValue): Option[String] = extractJString(j.dyn.socrata_id.dataset_id.?)

  def datasetName(j: JValue): Option[String] = extractJString(j.dyn.resource.name.?)

  def domainId(j: JValue): Option[Int] = j.dyn.socrata_id.domain_id.! match {
    case jn: JNumber => Option(jn.toInt)
    case JArray(elems) => elems.lastOption.map(_.asInstanceOf[JNumber].toInt)
    case jv: JValue => throw new NoSuchElementException(s"Unexpected json value $jv")
  }

  def isPublic(j: JValue): Boolean = extractJBoolean(j.dyn.is_public.?).exists(identity)

  def isPublished(j: JValue): Boolean = extractJBoolean(j.dyn.is_published.?).exists(identity)

  def isDefaultView(j: JValue): Boolean = extractJBoolean(j.dyn.is_default_view.?).exists(identity)

  def previewImageId(j: JValue): Option[String] = extractJString(j.dyn.preview_image_id.?)

  def viewModerationApproved(j: JValue, domainSet: DomainSet, d: Domain): Boolean = {
    val literallyApproved = extractJBoolean(j.dyn.is_moderation_approved.?).exists(identity)

    (domainSet.contextIsModerated, d.moderationEnabled) match { // bee do bee do bee do bee do
      case (true, false) => isDefaultView(j)
      case (false, false) => isDefaultView(j) || literallyApproved || !isDatalens(j)
      case (_, true) => isDefaultView(j) || literallyApproved
    }
  }

  def routingApproved(j: JValue, domain: Domain): Boolean = {
    val d = domainId(j).getOrElse(throw new NoSuchElementException)
    j.dyn.approving_domain_ids.! match {
      case jn: JNumber => Some(jn.toInt == d)
      case JArray(elems) => Some(elems.map(_.asInstanceOf[JNumber].toInt).contains(d))
      case _ => None
    }
  }.exists(identity) || !domain.routingApprovalEnabled

  val datalensTypeStrings = List(TypeDatalenses, TypeDatalensCharts, TypeDatalensMaps).map(_.singular)
  def isDatalens(j: JValue): Boolean = extractJString(j.dyn.datatype.?).exists(t => datalensTypeStrings.contains(t))

  // I know, let's implement visibility calculations in another place, great idea right!
  private def calculateAnonymousVisibility(j: JValue, domainSet: DomainSet): JBoolean = {
    val domain = domainId(j).flatMap { i => domainSet.idMap.get(i) }

    val visibility = domain.map { d =>
      isPublic(j) && isPublished(j) && routingApproved(j, d) && viewModerationApproved(j, domainSet, d)
    }.exists(identity)

    JBoolean(visibility)
  }

  def documentSearchResult(
      j: JValue,
      domainIdCnames: Map[Int, String],
      locale: Option[String],
      score: Option[JNumber],
      visibility: Option[JBoolean])
    : Option[SearchResult] = {
    try {
      val scoreMap = score.map(s => Seq("score" -> s)).getOrElse(Seq.empty)

      val visibilityMap = visibility.map(v => Seq(visibleToAnonKey -> v)).getOrElse(Seq.empty)

      val linkMap = links(
        cname(domainIdCnames, j),
        locale,
        datatype(j),
        viewtype(j),
        datasetId(j).get,
        domainCategoryString(j),
        datasetName(j).get,
        previewImageId(j))

      Some(SearchResult(
        j.dyn.resource.!,
        Classification(
          categories(j),
          tags(j),
          domainCategory(j),
          domainTags(j),
          domainMetadata(j)),
        Map(esDomainType -> JString(cname(domainIdCnames, j))) ++ scoreMap ++ visibilityMap,
        linkMap.getOrElse("permalink", JString("")),
        linkMap.getOrElse("link", JString("")),
        linkMap.get("previewImageUrl"))
      )
    }
    catch { case e: Exception =>
      logger.info(e.getMessage)
      None
    }
  }

  // WARN: This will raise if a single document has a single missing path!
  def formatDocumentResponse(formatParams: FormatParamSet, domainSet: DomainSet, searchResponse: SearchResponse)
    : SearchResults[SearchResult] = {
    val domainIdCnames = domainSet.idMap.map { case (i, d) => i -> d.domainCname }
    val hits = searchResponse.getHits
    val searchResult = hits.hits().flatMap { hit =>
      val json = JsonReader.fromString(hit.sourceAsString())
      val score = if (formatParams.showScore) Some(JNumber(hit.score)) else None
      val visibility = if (formatParams.showVisibility) Some(calculateAnonymousVisibility(json, domainSet)) else None
      documentSearchResult(json, domainIdCnames, formatParams.locale, score, visibility)
    }
    SearchResults(searchResult, hits.getTotalHits)
  }
}
