package com.socrata.cetera.services

import javax.servlet.http.HttpServletResponse

import com.rojoma.json.v3.ast.{JValue, JArray, JString}
import com.rojoma.json.v3.io.JsonReader
import com.rojoma.json.v3.jpath.JPath
import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder

import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.{HttpRequest, HttpResponse, HttpService}
import org.elasticsearch.action.search.SearchResponse
import org.slf4j.LoggerFactory

import com.socrata.cetera.search.ElasticSearchClient
import com.socrata.cetera.util.QueryParametersParser
import com.socrata.cetera.util.{InternalTimings, SearchResultsWithTimings}

case class ResourceAndMetadata(resource:JValue, metadata:Map[String, JValue], link:JString)
object ResourceAndMetadata {
  implicit val jCodec = AutomaticJsonCodecBuilder[ResourceAndMetadata]
}

case class SearchResults(results:Seq[ResourceAndMetadata], timings:Option[InternalTimings] = None)
object SearchResults {
  implicit val jCodec = AutomaticJsonCodecBuilder[SearchResults]
}




class SearchService(elasticSearchClient: ElasticSearchClient) extends SimpleResource {
  lazy val logger = LoggerFactory.getLogger(classOf[SearchService])

  // Fails silently if path does not exist
  def extractResources(body: JValue): Stream[JValue] = {
    val jPath = new JPath(body)
    jPath.down("hits").down("hits").*.down("_source").finish
  }

  def formatSearchResults(searchResponse: SearchResponse): SearchResults = {
    val body = JsonReader.fromString(searchResponse.toString)
    val resources = extractResources(body)
    SearchResults(
      resources.map { r =>
        val cname = r.dyn("socrata_id").apply("domain_cname").!.cast[JArray].get.apply(0).cast[JString].get
        val datasetID = r.dyn("socrata_id").apply("dataset_id").!.cast[JString].get.string
        ResourceAndMetadata(r.dyn("resource").!, 
          Map("domain"->cname),
          JString(s"""${cname.string}/ux/dataset/${datasetID}"""))
      }
    )
  }

  // Failure cases are not handled, in particular actionGet() from ES throws
  def search(req: HttpRequest): HttpServletResponse => Unit = {
    val now = Timings.now()
    val logMsg = List[String]("[" + req.servletRequest.getMethod + "]",
      req.requestPathStr,
      req.queryStr.getOrElse("<no query params>"),
      "requested by",
      req.servletRequest.getRemoteHost).mkString(" -- ")
    logger.info(logMsg)

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

    val formattedResults = formatSearchResults(searchResponse).copy(timings = Some(InternalTimings(Timings.elapsedInMillis(now), Option(searchResponse.getTookInMillis()))))
      
    val payload = Json(formattedResults, pretty=true)

    OK ~> payload
  }

  override def get: HttpService = search
}
