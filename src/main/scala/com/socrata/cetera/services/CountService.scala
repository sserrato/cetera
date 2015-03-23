package com.socrata.cetera.services

import javax.servlet.http.HttpServletResponse
import scala.util.{Try, Success, Failure}

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.io.JsonReader
import com.rojoma.json.v3.jpath.JPath
import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.{HttpRequest, HttpService}
import org.elasticsearch.ElasticsearchException
import org.elasticsearch.action.search.SearchResponse
import org.slf4j.LoggerFactory

import com.socrata.cetera.search.ElasticSearchClient
import com.socrata.cetera.types.CeteraFieldType
import com.socrata.cetera.util.JsonResponses._
import com.socrata.cetera.util._

case class Count(domain: JValue, count: JValue)

object Count {
  implicit val jCodec = AutomaticJsonCodecBuilder[Count]
}

class CountService(elasticSearchClient: ElasticSearchClient) {
  lazy val logger = LoggerFactory.getLogger(classOf[CountService])

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
  def format(counts: Stream[JValue]): SearchResults[Count] =  {
    SearchResults(
      counts.map { c => Count(c.dyn("key").!, c.dyn("doc_count").!) }
    )
  }

  def aggregate(field: CeteraFieldType)(req: HttpRequest): HttpServletResponse => Unit = {
    val now = Timings.now()

    val params = QueryParametersParser(req)

    params match {
      case Right(params) =>
        val request = elasticSearchClient.buildCountRequest(
          field,
          params.searchQuery,
          params.domains,
          params.categories,
          params.tags,
          params.only
        )

        val response = Try(request.execute().actionGet())

        response match {
          case Success(res) =>
            val timings = InternalTimings(Timings.elapsedInMillis(now), Option(res.getTookInMillis()))
            val json = JsonReader.fromString(res.toString)
            val counts = extract(json)
            val formattedResults = format(counts).copy(timings = Some(timings))
            val logMsg = List[String]("[" + req.servletRequest.getMethod + "]",
              req.requestPathStr,
              req.queryStr.getOrElse("<no query params>"),
              "requested by",
              req.servletRequest.getRemoteHost,
              s"""TIMINGS ## ESTime : ${timings.searchMillis.getOrElse(-1)} ## ServiceTime : ${timings.serviceMillis}""").mkString(" -- ")
            logger.info(logMsg)
            val payload = Json(formattedResults, pretty=true)
            OK ~> payload

          case Failure(ex) =>
            InternalServerError ~> jsonError(s"Database error: ${ex.getMessage}")
        }

      case Left(errors) =>
        val msg = errors.map(_.message).mkString(", ")
        BadRequest ~> jsonError(s"Invalid query parameters: ${msg}")
    }
  }

  case class Service(field: CeteraFieldType) extends SimpleResource {
    override def get: HttpService = aggregate(field)
  }
}
