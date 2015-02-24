package com.socrata.cetera.services

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.io.JsonReader
import com.rojoma.json.v3.jpath.JPath
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.{HttpRequest, HttpResponse, HttpService}
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.index.query.QueryBuilders
import org.slf4j.LoggerFactory

import com.socrata.cetera.util.JsonResponses.jsonError

class SearchService(client: TransportClient) extends SimpleResource {
  lazy val logger = LoggerFactory.getLogger(classOf[SearchService])

  // This will fail silently if the path does not exist
  def extractResources(body: JValue): Stream[JValue] = {
    val jPath = new JPath(body)
    jPath.down("hits").down("hits").*.down("_source").down("resource").finish
  }

  // There are many unhandled failure cases here
  def performSearch(query: String, offset: Int = 0, limit: Int = 100): Map[String, Stream[Map[String, JValue]]] = {
    val response = client.prepareSearch()
      .setQuery(QueryBuilders.matchQuery("_all", query))
      .setFrom(offset).setSize(limit)
      .execute()
      .actionGet()

    val body = JsonReader.fromString(response.toString)
    val resources = extractResources(body)

    Map("results" -> resources.map { r => Map("resource" -> r) })
  }

  def extractQuery(req: HttpRequest): String = {
    req.queryParameters.get("q") match {
      case Some(s) => s
      case None => ""
    }
  }

  override def get: HttpService = { req: HttpRequest =>
    val logMsg = List[String]("[" + req.servletRequest.getMethod + "]",
      req.requestPathStr,
      req.queryStr.getOrElse("<no query params>"),
      "requested by",
      req.servletRequest.getRemoteHost).mkString(" -- ")

    logger.info(logMsg)

    val query = extractQuery(req)
    val results = performSearch(query)
    val payload = Json(results, pretty=true)

    OK ~> payload
  }
}
