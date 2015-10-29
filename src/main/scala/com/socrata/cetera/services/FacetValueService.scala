package com.socrata.cetera.services

import com.socrata.cetera._
import com.socrata.cetera.search.ElasticSearchClient
import com.socrata.cetera.types._
import com.socrata.cetera.util.JsonResponses._
import com.socrata.cetera.util._
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.{HttpRequest, HttpResponse}
import org.elasticsearch.search.aggregations.bucket.nested.Nested
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

class FacetValueService(elasticSearchClient: Option[ElasticSearchClient]) {
  lazy val logger = LoggerFactory.getLogger(classOf[FacetValueService])

  // $COVERAGE-OFF$ jetty wiring
  def listValues(cname: String, facet: String)(req: HttpRequest): HttpResponse = {
    QueryParametersParser(req) match {
      case Left(errors) =>
        val msg = errors.map(_.message).mkString(", ")
        BadRequest ~> HeaderAclAllowOriginAll ~> jsonError(s"Invalid query parameters: $msg")
      case Right(params) =>
        try {
          val (facetValues, timings) = doListValues(cname, Option(facet))
          logger.info(LogHelper.formatRequest(req, timings))
          OK ~> HeaderAclAllowOriginAll ~> Json(facetValues)
        } catch {
          case e: Exception =>
            val esError = ElasticsearchError(e)
            logger.error("Database error: ${esError.getMessage}")
            InternalServerError ~> HeaderAclAllowOriginAll ~> jsonError(s"Database error", esError)
        }
    }
  }
  // $COVERAGE-ON$

  def doListValues(cname: String, facet: Option[String]): (Map[String,Seq[ValueCount]],InternalTimings) = {
    val startMs = Timings.now()

    val request = elasticSearchClient.getOrElse(throw new NullPointerException)
      .buildFacetValueRequest(cname, facet)
    logger.debug("issuing request to elasticsearch: " + request.toString)
    val res = request.execute().actionGet()
    val timings = InternalTimings(Timings.elapsedInMillis(startMs), Option(res.getTookInMillis))
    val hits = res.getAggregations.get("metadata").asInstanceOf[Nested]
      .getAggregations.get("kvp").asInstanceOf[Terms]
      .getBuckets.asScala.map(b => ValueCount(b.getKey, b.getDocCount)).toSeq

    (Map(facet.getOrElse("*") -> hits), timings)
  }
}
