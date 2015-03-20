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
import com.socrata.cetera.util.QueryParametersParser
import com.socrata.cetera.util.{InternalTimings, SearchResults}

case class DomainCount(domain: JValue, count: JValue)

object DomainCount {
  implicit val jCodec = AutomaticJsonCodecBuilder[DomainCount]
}

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
  def format(counts: Stream[JValue]): SearchResults[DomainCount] =  {
    SearchResults(
      counts.map { c => DomainCount(c.dyn("key").!, c.dyn("doc_count").!) }
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

    val timings = InternalTimings(Timings.elapsedInMillis(now), Option(response.getTookInMillis()))
    val results = format(counts).copy(timings = Some(timings))

    val logMsg = List[String]("[" + req.servletRequest.getMethod + "]",
      req.requestPathStr,
      req.queryStr.getOrElse("<no query params>"),
      "requested by",
      req.servletRequest.getRemoteHost,
      s"""TIMINGS ## ESTime : ${timings.searchMillis.getOrElse(-1)} ## ServiceTime : ${timings.serviceElapsedMillis}""").mkString(" -- ")
    logger.info(logMsg)
    val payload = Json(results, pretty=true)

    OK ~> payload
  }

  override def get: HttpService = aggregate
}
