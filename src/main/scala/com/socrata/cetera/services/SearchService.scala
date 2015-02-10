package com.socrata.cetera.services

import com.rojoma.json.v3.io.{CompactJsonWriter, JsonReader}
import com.rojoma.json.v3.util.JsonUtil.renderJson
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.{HttpRequest, HttpResponse}
import org.elasticsearch.client.transport.TransportClient
import org.slf4j.LoggerFactory

import com.socrata.cetera.util.JsonResponses.jsonMessage

class SearchService(client: TransportClient) {
  lazy val logger = LoggerFactory.getLogger(classOf[TransportClient])

  def performSearch(req: HttpRequest): HttpResponse = {
    val response = client.prepareSearch().execute().actionGet();
    val body = JsonReader.fromString(response.toString)

    OK ~> Json(body)
  }

  object Service extends SimpleResource {
    override def get: HttpRequest => HttpResponse = performSearch
  }
}
