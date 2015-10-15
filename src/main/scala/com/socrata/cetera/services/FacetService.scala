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

class FacetService(elasticSearchClient: Option[ElasticSearchClient]) {
  lazy val logger = LoggerFactory.getLogger(classOf[FacetService])

  def aggregate(cname: String)(req: HttpRequest): HttpResponse = {
    val startMs = Timings.now()

    QueryParametersParser(req) match {
      case Left(errors) =>
        val msg = errors.map(_.message).mkString(", ")
        BadRequest ~> HeaderAclAllowOriginAll ~> jsonError(s"Invalid query parameters: $msg")
      case Right(params) =>
        val request = elasticSearchClient.getOrElse(throw new NullPointerException).buildFacetRequest(cname)
        logger.debug("issuing request to elasticsearch: " + request.toString)

        try {
          val res = request.execute().actionGet()
          val timings = InternalTimings(Timings.elapsedInMillis(startMs), Option(res.getTookInMillis))
          val logMsg = LogHelper.formatRequest(req, timings)
          logger.info(logMsg)

          val hits = res.getAggregations.asMap().asScala
          val categories = hits.get("categories").get.asInstanceOf[Terms]
            .getBuckets.asScala.map(b => FacetCount(b.getKey, b.getDocCount)).toSeq
          val tags = hits.get("tags").get.asInstanceOf[Terms]
            .getBuckets.asScala.map(b => FacetCount(b.getKey, b.getDocCount)).toSeq
          val metadata = hits.get("metadata").get.asInstanceOf[Nested]
            .getAggregations.get("kvp").asInstanceOf[Terms]
            .getBuckets.asScala.map(b => FacetCount(b.getKey, b.getDocCount)).toSeq

          val facets: Map[String,Seq[FacetCount]] = Map(
            "categories" -> categories,
            "tags"       -> tags,
            "metadata"   -> metadata
          )

          OK ~> HeaderAclAllowOriginAll ~> Json(facets)
        } catch {
          case e: Exception =>
            InternalServerError ~> HeaderAclAllowOriginAll ~> jsonError(s"Database error", e)
        }
    }
  }
}
