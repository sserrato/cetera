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
  def format(counts: Stream[JValue]) = {
    Map("results" ->
      Map("domain_counts" ->
        counts.map { c => Map("domain" -> c.dyn("key").!,
                              "count" -> c.dyn("doc_count").!) }
      )
    )
  }

  def prepare(searchResponse: SearchResponse) = {
    val body = JsonReader.fromString(searchResponse.toString)
    val counts = extract(body)
    format(counts)
  }

  def aggregate(req: HttpRequest): HttpServletResponse => Unit = {
    val params = QueryParametersParser(req)

    val domainRequest = elasticSearchClient.buildDomainRequest(
      params.searchQuery,
      params.domains,
      params.categories,
      params.tags,
      params.only
    )

    val response = domainRequest.execute().actionGet()
    val results = prepare(response)

    val payload = Json(results, pretty=true)
    OK ~> payload
  }

  override def get: HttpService = aggregate
}
