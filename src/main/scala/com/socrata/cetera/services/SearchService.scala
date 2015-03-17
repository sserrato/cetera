package com.socrata.cetera.services

import javax.servlet.http.HttpServletResponse

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.io.JsonReader
import com.rojoma.json.v3.jpath.JPath
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.{HttpRequest, HttpResponse, HttpService}
import org.elasticsearch.action.search.SearchResponse
import org.slf4j.LoggerFactory

import com.socrata.cetera.search.ElasticSearchClient
import com.socrata.cetera.util.QueryParametersParser

class SearchService(elasticSearchClient: ElasticSearchClient) extends SimpleResource {
  lazy val logger = LoggerFactory.getLogger(classOf[SearchService])

  // Fails silently if path does not exist
  def extractResources(body: JValue): Stream[JValue] = {
    val jPath = new JPath(body)
    jPath.down("hits").down("hits").*.down("_source").down("resource").finish
  }

  def formatSearchResults(searchResponse: SearchResponse): Map[String, Stream[Map[String, JValue]]] = {
    val body = JsonReader.fromString(searchResponse.toString)
    val resources = extractResources(body)
    Map("results" ->
      resources.map { r =>
        Map("resource" -> r) })
  }

  // Failure cases are not handled, in particular actionGet() from ES throws
  def search(req: HttpRequest): HttpServletResponse => Unit = {
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

    val formattedResults = formatSearchResults(searchResponse)
    val payload = Json(formattedResults, pretty=true)

    OK ~> payload
  }

  override def get: HttpService = search
}
