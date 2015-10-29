package com.socrata.cetera.services

import com.socrata.cetera._
import com.socrata.cetera.search.ElasticSearchClient
import com.socrata.cetera.types._
import com.socrata.cetera.util.JsonResponses._
import com.socrata.cetera.util._
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.{HttpRequest, HttpResponse, HttpService}
import org.elasticsearch.search.aggregations.bucket.nested.Nested
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

class FacetService(elasticSearchClient: Option[ElasticSearchClient]) {
  lazy val logger = LoggerFactory.getLogger(classOf[FacetService])

  // $COVERAGE-OFF$ jetty wiring
  case class Service(cname: String) extends SimpleResource {
    override def get: HttpService = aggregate(cname)
  }

  def aggregate(cname: String)(req: HttpRequest): HttpResponse = {
    QueryParametersParser(req) match {
      case Left(errors) =>
        val msg = errors.map(_.message).mkString(", ")
        BadRequest ~> HeaderAclAllowOriginAll ~> jsonError(s"Invalid query parameters: $msg")
      case Right(params) =>
        try {
          val (facets, timings) = doAggregate(cname)
          logger.info(LogHelper.formatRequest(req, timings))
          OK ~> HeaderAclAllowOriginAll ~> Json(facets)
        } catch {
          case e: Exception =>
            val esError = ElasticsearchError(e)
            logger.error(s"Database error: ${esError.getMessage}")
            InternalServerError ~> HeaderAclAllowOriginAll ~> jsonError(s"Database error", esError)
        }
    }
  }
  // $COVERAGE-ON$

  def doAggregate(cname: String): (Map[String,Seq[FacetCount]], InternalTimings) = {
    val startMs = Timings.now()

    val request = elasticSearchClient.getOrElse(throw new NullPointerException).buildFacetRequest(cname)
    val res = request.execute().actionGet()
    val aggs = res.getAggregations.asMap().asScala
    val categories = aggs("categories").asInstanceOf[Terms]
      .getBuckets.asScala.map(b => FacetCount(b.getKey, b.getDocCount)).toSeq
    val tags = aggs("tags").asInstanceOf[Terms]
      .getBuckets.asScala.map(b => FacetCount(b.getKey, b.getDocCount)).toSeq
    val metadata = aggs("metadata").asInstanceOf[Nested]
      .getAggregations.get("kvp").asInstanceOf[Terms]
      .getBuckets.asScala.map(b => FacetCount(b.getKey, b.getDocCount)).toSeq

    val facets: Map[String,Seq[FacetCount]] = Map(
      "categories" -> categories,
      "tags"       -> tags,
      "metadata"   -> metadata
    )
    val timings = InternalTimings(Timings.elapsedInMillis(startMs), Option(res.getTookInMillis))
    (facets, timings)
  }
}
