package com.socrata.cetera.services

import com.rojoma.json.v3.util.JsonUtil
import com.socrata.cetera._
import com.socrata.cetera.search.ElasticSearchClient
import com.socrata.cetera.types._
import com.socrata.cetera.util.JsonResponses._
import com.socrata.cetera.util._
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.{HttpRequest, HttpResponse}
import org.slf4j.LoggerFactory

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

          val hits = res.getHits.hits()
            .map(h => JsonUtil.parseJson[FacetHit](h.sourceAsString()) match {
              case Left(decodeError) => throw new RuntimeException(decodeError.english)
              case Right(facetHit) => facetHit
            })

          val facets: Map[String,Seq[String]] = Map(
            "categories" -> hits.map(_.customerCategory.getOrElse("")).filter(_.nonEmpty).distinct.sorted,
            "tags"       -> hits.flatMap(_.customerTags.getOrElse(Seq.empty[String])).distinct.sorted,
            "metadata"   -> hits.flatMap(_.customerMetadataFlattened.map(_.map(_.key)).getOrElse(Nil)).distinct.sorted
          )

          OK ~> HeaderAclAllowOriginAll ~> Json(facets)
        } catch {
          case e: Exception =>
            InternalServerError ~> HeaderAclAllowOriginAll ~> jsonError(s"Database error", e)
        }
    }
  }
}
