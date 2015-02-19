package com.socrata.cetera.services

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.io.JsonReader
import com.rojoma.json.v3.jpath.JPath
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.{HttpRequest, HttpResponse, HttpService}
import org.elasticsearch.client.transport.TransportClient
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
  def performSearch: Map[String, Stream[Map[String, JValue]]] = {
    val response = client.prepareSearch().execute().actionGet();
    val body = JsonReader.fromString(response.toString)
    val resources = extractResources(body)

    Map("results" -> resources.map { r => Map("resource" -> r) })
  }

  override def get: HttpService = { req: HttpRequest =>
    val results = performSearch
    OK ~> Json(results, pretty=true)
  }
}
