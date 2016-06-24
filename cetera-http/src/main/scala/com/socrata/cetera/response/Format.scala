package com.socrata.cetera.response

import com.rojoma.json.v3.ast.{JNumber, _}
import com.rojoma.json.v3.codec.DecodeError
import com.rojoma.json.v3.io.JsonReader
import com.rojoma.json.v3.jpath.JPath
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, JsonKeyStrategy, Strategy}
import org.elasticsearch.action.search.SearchResponse
import org.slf4j.LoggerFactory

import com.socrata.cetera._
import com.socrata.cetera.types._

object Format {
  lazy val logger = LoggerFactory.getLogger(Format.getClass)
  val UrlSegmentLengthLimit = 50

  def hyphenize(text: String): String = Option(text) match {
    case Some(s) if s.nonEmpty => s.replaceAll("[^\\p{L}\\p{N}_]+", "-").take(UrlSegmentLengthLimit)
    case _ => "-"
  }

  def links(
      cname: String,
      datatype: Option[Datatype],
      viewtype: Option[String],
      datasetId: String,
      datasetCategory: Option[String],
      datasetName: String): Map[String, JString] = {
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

    Map(
      "permalink" ->JString(s"https://$cname/$perma/$datasetId"),
      "link" -> JString(s"https://$cname/$pretty/$datasetId")
    )
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
    val id: Option[Int] = j.dyn.socrata_id.domain_id.! match {
      case jn: JNumber => Option(jn.toInt)
      case JArray(elems) => elems.lastOption.map(_.asInstanceOf[JNumber].toInt)
      case jv: JValue => throw new NoSuchElementException(s"Unexpected json value $jv")
    }
    id.flatMap { i =>
      domainCnames.get(i)
    }.getOrElse("") // if no domain was found, default to blank string
  }

  private def extractJString(decoded: Either[DecodeError, JValue]): Option[String] =
    decoded.fold(_ => None, {
      case JString(s) => Option(s)
      case _ => None
    })

  def datatype(j: JValue): Option[Datatype] =
    extractJString(j.dyn.datatype.?).flatMap(s => Datatype(s))

  def viewtype(j: JValue): Option[String] = extractJString(j.dyn.viewtype.?)

  def datasetId(j: JValue): Option[String] = extractJString(j.dyn.socrata_id.dataset_id.?)

  def datasetName(j: JValue): Option[String] = extractJString(j.dyn.resource.name.?)

  def documentSearchResult(
      j: JValue,
      domainIdCnames: Map[Int, String],
      score: Option[JNumber])
    : Option[SearchResult] = {
    try {
      val scoreMap = score.map(s => Seq("score" -> s)).getOrElse(Seq.empty)

      val linkMap = links(
        cname(domainIdCnames, j),
        datatype(j),
        viewtype(j),
        datasetId(j).get,
        domainCategoryString(j),
        datasetName(j).get)

      Some(SearchResult(
        j.dyn.resource.!,
        Classification(
          categories(j),
          tags(j),
          domainCategory(j),
          domainTags(j),
          domainMetadata(j)),
        Map(esDomainType -> JString(cname(domainIdCnames, j))) ++ scoreMap,
        linkMap.getOrElse("permalink", JString("")),
        linkMap.getOrElse("link", JString(""))
      ))
    }
    catch { case e: Exception =>
      logger.info(e.getMessage)
      None
    }
  }

  // WARN: This will raise if a single document has a single missing path!
  def formatDocumentResponse(
      domainIdCnames: Map[Int, String],
      showScore: Boolean,
      searchResponse: SearchResponse)
    : SearchResults[SearchResult] = {
    val hits = searchResponse.getHits
    val searchResult = hits.hits().flatMap { hit =>
      val json = JsonReader.fromString(hit.sourceAsString())
      val score = if (showScore) Some(JNumber(hit.score)) else None
      documentSearchResult(json, domainIdCnames, score)
    }
    SearchResults(searchResult, hits.getTotalHits)
  }
}
