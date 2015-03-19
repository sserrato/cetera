package com.socrata.cetera.services

import javax.servlet.http.HttpServletResponse

import com.rojoma.json.v3.ast.JValue
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
import com.socrata.cetera.util.{InternalTimings, SearchResults, QueryParametersParser}

case class TagCount(tag: JValue, count: JValue)

object TagCount {
  implicit val jCodec = AutomaticJsonCodecBuilder[TagCount]
}

class TagsService(elasticSearchClient: ElasticSearchClient) extends SimpleResource {
  lazy val logger = LoggerFactory.getLogger(classOf[TagsService])

  // Possibly belongs in the client
  // Fails silently if path does not exist
  def extract(body: JValue): Stream[JValue] = {
    val jPath = new JPath(body)
    jPath
      .down("aggregations")
      .down("counts")
      .down("buckets")
      .*
      .finish
  }

  // Unhandled exception on missing key
  def format(counts: Stream[JValue]): SearchResults[TagCount] =  {
    SearchResults(
      counts.map { c => TagCount(c.dyn("key").!, c.dyn("doc_count").!) }
    )
  }

  def aggregate(req: HttpRequest): HttpServletResponse => Unit = {
    val now = Timings.now()
    val params = QueryParametersParser(req)

    val request = elasticSearchClient.buildCountRequest(
      "animl_annotations.tag_names.raw",
      params.searchQuery,
      params.tags,
      params.categories,
      params.tags,
      params.only
    )
    val response = request.execute().actionGet()

    val json = JsonReader.fromString(response.toString)
    val counts = extract(json)

    val results = format(counts).copy(
      timings = Some(
        InternalTimings(
          Timings.elapsedInMillis(now),
          Option(response.getTookInMillis())
        )
      )
    )

    val payload = Json(results, pretty=true)

    OK ~> payload
  }

  override def get: HttpService = aggregate
}

