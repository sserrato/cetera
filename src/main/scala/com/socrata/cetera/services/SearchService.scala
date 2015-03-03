package com.socrata.cetera.services

import javax.servlet.http.HttpServletResponse

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.io.JsonReader
import com.rojoma.json.v3.jpath.JPath
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.{HttpRequest, HttpResponse, HttpService}
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchResponse}
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.index.query.{FilterBuilders, QueryBuilders}
import org.slf4j.LoggerFactory

class SearchService(client: TransportClient) extends SimpleResource {
  lazy val logger = LoggerFactory.getLogger(classOf[SearchService])

  def buildSearchRequest(searchQuery: Option[String],
                         domain: Option[String] = None,
                         only: Option[String] = None,
                         offset: Int = 0,
                         limit: Int = 100): SearchRequestBuilder = {

    val filteredQuery = {
      val matchQuery = searchQuery match {
        case None => QueryBuilders.matchAllQuery()
        case Some(sq) => QueryBuilders.matchQuery("_all", sq)
      }

      val domainFilter = domain match {
        case None => FilterBuilders.matchAllFilter()
        case Some(d) => FilterBuilders.termFilter("domain_cname_exact", d)
      }

      QueryBuilders.filteredQuery(
        matchQuery,
        domainFilter
      )
    }

    // Imperative-style builder function
    client.prepareSearch()
      .setTypes(only.toList:_*)
      .setQuery(filteredQuery)
      .setFrom(offset)
      .setSize(limit)
  }

  // Fails silently if path does not exist
  def extractResources(body: JValue): Stream[JValue] = {
    val jPath = new JPath(body)
    jPath.down("hits").down("hits").*.down("_source").down("resource").finish
  }

  def formatSearchResults(searchResponse: SearchResponse): Map[String, Stream[Map[String, JValue]]] = {
    val body = JsonReader.fromString(searchResponse.toString)
    val resources = extractResources(body)
    Map("results" -> resources.map { r => Map("resource" -> r) })
  }

  // Failure cases are not handled, in particular actionGet() from ES throws
  def search(req: HttpRequest): HttpServletResponse => Unit = {
    val logMsg = List[String]("[" + req.servletRequest.getMethod + "]",
      req.requestPathStr,
      req.queryStr.getOrElse("<no query params>"),
      "requested by",
      req.servletRequest.getRemoteHost).mkString(" -- ")

    logger.info(logMsg)

    val searchQuery = req.queryParameters.get("q")
    val domain = req.queryParameters.get("domain")
    val only = req.queryParameters.get("only")

    val searchRequest = buildSearchRequest(
      searchQuery,
      domain,
      only
    )

    val searchResponse = searchRequest.execute().actionGet()

    val formattedResults = formatSearchResults(searchResponse)
    val payload = Json(formattedResults, pretty=true)

    OK ~> payload
  }

  override def get: HttpService = search
}
