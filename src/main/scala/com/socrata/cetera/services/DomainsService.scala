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
import com.socrata.cetera.util.{InternalTimings, DomainResultsWithTimings}

class DomainsService(elasticSearchClient: ElasticSearchClient) extends SimpleResource {
  lazy val logger = LoggerFactory.getLogger(classOf[DomainsService])

  // Possibly belongs in the client
  // Fails silently if path does not exist
  def extract(body: JValue): Stream[JValue] = {
    val jPath = new JPath(body)
    jPath
      .down("aggregations")
      .down("domain_resources_count")
      .down("buckets")
      .*
      .finish
  }

  // Unhandled exception on missing key
  def format(counts: Stream[JValue]): Map[String, Map[String, Stream[Map[String, JValue]]]] = {
    Map("results" ->
      Map("domain_counts" ->
        counts.map { c => Map("domain" -> c.dyn("key").!,
                              "count" -> c.dyn("doc_count").!) }
      )
    )
  }

  def aggregate(req: HttpRequest): HttpServletResponse => Unit = {
    val now = Timings.now()
    val params = QueryParametersParser(req)

    val domainRequest = elasticSearchClient.buildDomainRequest(
      params.searchQuery,
      params.domains,
      params.categories,
      params.tags,
      params.only
    )
    val response = domainRequest.execute().actionGet()

    val json = JsonReader.fromString(response.toString)
    val counts = extract(json)
    val results = format(counts)
    val formattedResultesWithTimings = DomainResultsWithTimings(
      InternalTimings(Timings.elapsedInMillis(now), Option(response.getTookInMillis())),
      results)
    val payload = Json(formattedResultesWithTimings, pretty=true)

    OK ~> payload
  }

  override def get: HttpService = aggregate
}
