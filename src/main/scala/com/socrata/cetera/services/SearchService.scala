package com.socrata.cetera.services

import javax.servlet.http.HttpServletResponse

import com.rojoma.json.v3.ast.{JValue, JArray, JString}
import com.rojoma.json.v3.io.JsonReader
import com.rojoma.json.v3.jpath.JPath
import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.{HttpRequest, HttpService}
import org.elasticsearch.action.search.SearchResponse
import org.slf4j.LoggerFactory

import com.socrata.cetera.search.ElasticSearchClient
import com.socrata.cetera.util._

case class Classification(categories: JValue, tags: JValue)

object Classification {
  implicit val jCodec = AutomaticJsonCodecBuilder[Classification]
}

case class SearchResult(resource: JValue, classification: Classification, metadata: Map[String, JValue], link: JString)
object SearchResult {
  implicit val jCodec = AutomaticJsonCodecBuilder[SearchResult]
}

class SearchService(elasticSearchClient: ElasticSearchClient) extends SimpleResource {
  lazy val logger = LoggerFactory.getLogger(classOf[SearchService])

  // Fails silently if path does not exist
  def extract(body: JValue): Stream[JValue] = {
    val jPath = new JPath(body)
    jPath
      .down("hits")
      .down("hits")
      .*
      .down("_source")
      .finish
  }

  def format(searchResponse: SearchResponse): SearchResults[SearchResult] = {
    val body = JsonReader.fromString(searchResponse.toString)
    val resources = extract(body)
    SearchResults(
      resources.map { r =>
        val cname = r.dyn("socrata_id").apply("domain_cname").!.cast[JArray].get.apply(0).cast[JString].get
        val datasetID = r.dyn("socrata_id").apply("dataset_id").!.cast[JString].get.string
        val pageID = r.dyn("socrata_id").apply("page_id").?
        val catagories =r.dyn("animl_annotations").apply("category_names").!
        val tags =r.dyn("animl_annotations").apply("tag_names").!
        val link = pageID match {
          case Right(pgId) =>  JString(s"""${cname.string}/view/${pgId.cast[JString].get.string}""")
          case _ => JString(s"""${cname.string}/ux/dataset/${datasetID}""")
        }
        SearchResult(r.dyn("resource").!,
          Classification(catagories, tags),
          Map("domain"->cname),
          link)
      }
    )
  }

  // Failure cases are not handled, in particular actionGet() from ES throws
  def search(req: HttpRequest): HttpServletResponse => Unit = {
    val now = Timings.now()

    val params = QueryParametersParser(req)

    val searchRequest = elasticSearchClient.buildSearchRequest(
      params.searchQuery,
      params.domains,
      params.categories,
      params.tags,
      params.only,
      params.offset,
      params.limit
    )
    val searchResponse = searchRequest.execute().actionGet()

    val timings = InternalTimings(Timings.elapsedInMillis(now), Option(searchResponse.getTookInMillis()))
    val formattedResults = format(searchResponse).copy(timings = Some(timings))

    val logMsg = List[String]("[" + req.servletRequest.getMethod + "]",
      req.requestPathStr,
      req.queryStr.getOrElse("<no query params>"),
      "requested by",
      req.servletRequest.getRemoteHost,
      s"""TIMINGS ## ESTime : ${timings.searchMillis.getOrElse(-1)} ## ServiceTime : ${timings.serviceMillis}""").mkString(" -- ")
    logger.info(logMsg)

    val payload = Json(formattedResults, pretty=true)

    OK ~> payload
  }

  override def get: HttpService = search
}
