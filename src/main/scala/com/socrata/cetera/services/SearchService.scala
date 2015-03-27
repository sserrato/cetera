package com.socrata.cetera.services

import javax.servlet.http.HttpServletResponse
import scala.util.{Try, Success, Failure}

import com.rojoma.json.v3.ast.{JValue, JArray, JString}
import com.rojoma.json.v3.io.JsonReader
import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.{HttpRequest, HttpService}
import org.elasticsearch.action.search.SearchResponse
import org.slf4j.LoggerFactory

import com.socrata.cetera.search.ElasticSearchClient
import com.socrata.cetera.util.JsonResponses._
import com.socrata.cetera.util._

case class Classification(categories: JValue, tags: JValue)

object Classification {
  implicit val jCodec = AutomaticJsonCodecBuilder[Classification]
}

case class SearchResult(resource: JValue,
                        classification: Classification,
                        metadata: Map[String, JValue],
                        link: JString)

object SearchResult {
  implicit val jCodec = AutomaticJsonCodecBuilder[SearchResult]
}

class SearchService(elasticSearchClient: ElasticSearchClient) extends SimpleResource {
  lazy val logger = LoggerFactory.getLogger(classOf[SearchService])

  def format(searchResponse: SearchResponse): SearchResults[SearchResult] = {
    val resources = searchResponse.getHits().hits()
    SearchResults(
      resources.map { hit =>
        val source = hit.sourceAsString()
        val r = JsonReader.fromString(source)

        val categories = r.dyn.animl_annotations.category_names.? match {
          case Right(cats) => cats
          case Left(error) => JArray.canonicalEmpty // for consistent return body
        }

        val tags = r.dyn.animl_annotations.tag_names.? match {
          case Right(tags) => tags
          case Left(error) => JArray.canonicalEmpty // for consistent return body
        }

        // TODO: Fix production schema to make domain_cname an array again
        // and add back .last.asInstanceOf[JArray]
        val cname = r.dyn.socrata_id.domain_cname.! match {
          case JString(string) => string
          case JArray(elems) => elems.last.asInstanceOf[JString].string
        }

        val datasetID = r.dyn.socrata_id.dataset_id.!.asInstanceOf[JString]

        val pageID = r.dyn.socrata_id.page_id.?

        val link = pageID match {
          case Right(pgId) =>
            JString(s"""https://${cname}/view/${pgId.asInstanceOf[JString].string}""")
          case _ =>
            JString(s"""https://${cname}/ux/dataset/${datasetID.string}""")
        }

        SearchResult(
          r.dyn.resource.!,
          Classification(categories, tags),
          Map("domain" -> JString(cname)),
          link
        )
      }
    )
  }

  def search(req: HttpRequest): HttpServletResponse => Unit = {
    val now = Timings.now()

    QueryParametersParser(req) match {
      case Right(params) =>
        val request = elasticSearchClient.buildSearchRequest(
          params.searchQuery,
          params.domains,
          params.categories,
          params.tags,
          params.only,
          params.offset,
          params.limit
        )

        val response = Try(request.execute().actionGet())

        response match {
          case Success(res) =>
            val timings = InternalTimings(Timings.elapsedInMillis(now), Option(res.getTookInMillis()))
            val count = res.getHits().getTotalHits()
            val formattedResults = format(res).copy(resultSetSize = Some(count), timings = Some(timings))
            val logMsg = List[String]("[" + req.servletRequest.getMethod + "]",
              req.requestPathStr,
              req.queryStr.getOrElse("<no query params>"),
              "requested by",
              req.servletRequest.getRemoteHost,
              s"""TIMINGS ## ESTime : ${timings.searchMillis.getOrElse(-1)} ## ServiceTime : ${timings.serviceMillis}""").mkString(" -- ")
            logger.info(logMsg)
            val payload = Json(formattedResults, pretty=true)
            OK ~> Header("Access-Control-Allow-Origin", "*") ~> payload

          case Failure(ex) =>
            InternalServerError ~> Header("Access-Control-Allow-Origin", "*") ~> jsonError(s"Database error: ${ex.getMessage}")
        }

      case Left(errors) =>
        val msg = errors.map(_.message).mkString(", ")
        BadRequest ~> Header("Access-Control-Allow-Origin", "*") ~> jsonError(s"Invalid query parameters: ${msg}")
    }
  }

  object Service extends SimpleResource {
    override def get: HttpService = search
  }
}
