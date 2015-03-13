package com.socrata.cetera.services

import javax.servlet.http.HttpServletResponse

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.io.JsonReader
import com.rojoma.json.v3.jpath.JPath
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.{HttpRequest, HttpResponse, HttpService}
import org.slf4j.LoggerFactory

import com.socrata.cetera.search.ElasticSearchClient
import com.socrata.cetera.util.JsonResponses.jsonMessage
import com.socrata.cetera.util.QueryParametersParser

class DomainsService(elasticSearchClient: ElasticSearchClient) extends SimpleResource {
  lazy val logger = LoggerFactory.getLogger(classOf[DomainsService])

  def aggregate(req: HttpRequest): HttpServletResponse => Unit = {
    val params = QueryParametersParser(req)

    val domainRequest = elasticSearchClient.buildDomainRequest(
      params.searchQuery,
      params.domains,
      params.categories,
      params.tags,
      params.only,
      params.offset,
      params.limit
    )

    val res = domainRequest.execute().actionGet().toString
    OK ~> Content("application/json", res)
  }

  override def get: HttpService = aggregate
}
