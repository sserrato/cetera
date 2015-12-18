package com.socrata.cetera.services

import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.codec.DecodeError
import com.rojoma.json.v3.io.JsonReader
import com.rojoma.json.v3.jpath.JPath
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, JsonKeyStrategy, Strategy}
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.{HttpResponse, HttpRequest, HttpService}
import org.elasticsearch.action.search.SearchResponse
import org.slf4j.LoggerFactory

import com.socrata.cetera._
import com.socrata.cetera.search.{DomainClient, DocumentClient}
import com.socrata.cetera.types._
import com.socrata.cetera.util.JsonResponses._
import com.socrata.cetera.util._

@JsonKeyStrategy(Strategy.Underscore)
case class Classification(categories: Seq[JValue],
                          tags: Seq[JValue],
                          domainCategory: Option[JValue],
                          domainTags: Option[JValue],
                          domainMetadata: Option[JValue])

object Classification {
  implicit val jCodec = AutomaticJsonCodecBuilder[Classification]
}

case class SearchResult(resource: JValue,
                        classification: Classification,
                        metadata: Map[String, JValue],
                        permalink: JString,
                        link: JString)

object SearchResult {
  implicit val jCodec = AutomaticJsonCodecBuilder[SearchResult]
}

class SearchService(elasticSearchClient: DocumentClient, domainClient: DomainClient) extends SimpleResource {
  lazy val logger = LoggerFactory.getLogger(classOf[SearchService])

  // TODO: cetera-etl rename customer_blah to domain_blah
  private def domainCategory(j: JValue): Option[JValue] = j.dyn.customer_category.? match {
    case Left(e) => None
    case Right(jv) => Some(jv)
  }

  private def domainCategoryString(j: JValue): Option[String] =
    domainCategory(j).flatMap {
      case JString(s) => Option(s)
      case _ => None
    }

  private def domainTags(j: JValue): Option[JValue] = j.dyn.customer_tags.? match {
    case Left(e) => None
    case Right(jv) => Some(jv)
  }

  private def domainMetadata(j: JValue): Option[JValue] = j.dyn.customer_metadata_flattened.? match {
    case Left(e) => None
    case Right(jv) => Some(jv)
  }

  private def categories(j: JValue): Stream[JValue] =
    new JPath(j).down("animl_annotations").down("categories").*.down("name").finish.distinct

  private def tags(j: JValue): Stream[JValue] =
    new JPath(j).down("animl_annotations").down("tags").*.down("name").finish.distinct

  private def popularity(j: JValue): Option[(String,JValue)] =
    j.dyn.popularity.?.fold(_ => None, r => Option(("popularity", r)))

  private def updateFreq(j: JValue): Option[(String,JValue)] =
    j.dyn.update_freq.?.fold(_ => None, r => Option(("update_freq", r)))

  // TODO: When production mapping changes domain_cname back to array, and add back .last.asInstanceOf[JArray]
  private def cname(j: JValue): String = j.dyn.socrata_id.domain_cname.! match {
    case JString(string) => string
    case JArray(elems) => elems.lastOption.getOrElse(throw new NoSuchElementException).asInstanceOf[JString].string
    case jv: JValue => throw new NoSuchElementException(s"Unexpected json value $jv")
  }

  private def extractJString(decoded: Either[DecodeError, JValue]): Option[String] =
    decoded.fold(_ => None, {
      case JString(s) => Option(s)
      case _ => None
    })

  private def datatype(j: JValue): Option[DatatypeSimple] =
    extractJString(j.dyn.datatype.?).flatMap(s => DatatypeSimple(s))

  private def viewtype(j: JValue): Option[String] = extractJString(j.dyn.viewtype.?)

  private def datasetId(j: JValue): Option[String] = extractJString(j.dyn.socrata_id.dataset_id.?)

  private def datasetName(j: JValue): Option[String] = extractJString(j.dyn.resource.name.?)

  def format(showScore: Boolean,
             searchResponse: SearchResponse): SearchResults[SearchResult] = {
    SearchResults(searchResponse.getHits.hits().map { hit =>
      val json = JsonReader.fromString(hit.sourceAsString())

      val score = if (showScore) Seq("score" -> JNumber(hit.score)) else Seq.empty
      val links = SearchService.links(
        cname(json),
        datatype(json),
        viewtype(json),
        datasetId(json).getOrElse(throw new NoSuchElementException),
        domainCategoryString(json),
        datasetName(json).getOrElse(throw new NoSuchElementException))

      SearchResult(
        json.dyn.resource.!,
        Classification(
          categories(json),
          tags(json),
          domainCategory(json),
          domainTags(json),
          domainMetadata(json)),
        Map("domain" -> JString(cname(json))) ++ score,
        links.getOrElse("permalink", JString("")),
        links.getOrElse("link", JString(""))
      )
    })
  }

  def doSearch(queryParameters: Map[String,String]): (SearchResults[SearchResult], InternalTimings) = {
    val now = Timings.now()
    QueryParametersParser(queryParameters) match {
      case Left(errors) =>
        val msg = errors.map(_.message).mkString(", ")
        throw new IllegalArgumentException(s"Invalid query parameters: $msg")

      case Right(params) =>
        val domain = params.searchContext.flatMap(domainClient.getDomain)
        val res = elasticSearchClient.buildSearchRequest(
          params.searchQuery,
          params.domains,
          params.domainMetadata,
          domain,
          params.categories,
          params.tags,
          params.only,
          params.fieldBoosts,
          params.datatypeBoosts,
          params.minShouldMatch,
          params.slop,
          params.offset,
          params.limit
        ).execute.actionGet

        val timings = InternalTimings(Timings.elapsedInMillis(now), Option(res.getTookInMillis))
        val count = res.getHits.getTotalHits
        val formattedResults: SearchResults[SearchResult] = format(params.showScore, res)
          .copy(resultSetSize = Some(count), timings = Some(timings))

        (formattedResults, timings)
    }
  }

  // $COVERAGE-OFF$ jetty wiring
  def search(req: HttpRequest): HttpResponse = {
    try {
      val (formattedResults, timings) = doSearch(req.queryParameters)
          logger.info(LogHelper.formatRequest(req, timings))
          OK ~> HeaderAclAllowOriginAll ~> Json(formattedResults, pretty = true)
    } catch {
      case e: IllegalArgumentException =>
        logger.info(e.getMessage)
        BadRequest ~> HeaderAclAllowOriginAll ~> jsonError(e.getMessage)
      case e: Exception =>
        val esError = ElasticsearchError(e)
        logger.error(s"Database error: ${esError.getMessage}")
        InternalServerError ~> HeaderAclAllowOriginAll ~> jsonError(s"Database error", esError)
    }
  }

  object Service extends SimpleResource {
    override def get: HttpService = search
  }
  // $COVERAGE-ON$
}

object SearchService {
  def links(cname: String,
             datatype: Option[DatatypeSimple],
             viewtype: Option[String],
             datasetId: String,
             datasetCategory: Option[String],
             datasetName: String): Map[String,JString] = {
    val perma = (datatype, viewtype) match {
      case (Some(TypeStories), _)             => s"stories/s"
      case (Some(TypeDatalenses), _)          => s"view"
      case (_, Some(TypeDatalenses.singular)) => s"view"
      case _                                  => s"d"
    }

    val urlSegmentLengthLimit = 50
    def hyphenize(text: String): String = Option(text) match {
      case Some(s) if s.nonEmpty => s.replaceAll("[^\\p{L}\\p{N}_]+", "-").take(urlSegmentLengthLimit)
      case _ => "-"
    }
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
}
