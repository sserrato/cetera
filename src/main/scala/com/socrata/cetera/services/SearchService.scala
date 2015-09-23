package com.socrata.cetera.services

import javax.servlet.http.HttpServletResponse

import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.io.JsonReader
import com.rojoma.json.v3.jpath.JPath
import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder
import com.socrata.cetera.search.ElasticSearchClient
import com.socrata.cetera.util.JsonResponses._
import com.socrata.cetera.util._
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.{HttpRequest, HttpService}
import org.elasticsearch.action.search.SearchResponse
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

case class Classification(categories: Seq[JValue], tags: Seq[JValue], customerCategory: Option[JValue])

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

  def format(showFeatureVals: Boolean, showScore: Boolean, searchResponse: SearchResponse): SearchResults[SearchResult] = {
    val resources = searchResponse.getHits().hits()
    SearchResults(
      resources.map { hit =>
        val source = hit.sourceAsString()
        val json = JsonReader.fromString(source)

        val categories = new JPath(json).down("animl_annotations").down("categories").*.down("name").finish.distinct
        val tags = new JPath(json).down("animl_annotations").down("tags").*.down("name").finish.distinct        

        val updateFreq = json.dyn.update_freq.?.fold(_ => None, r => Option(("update_freq", r)))
        val popularity = json.dyn.popularity.?.fold(_ => None, r => Option(("popularity", r)))

        val score = if (showScore) Seq("score" -> JNumber(hit.score)) else Seq.empty

        val featureVals = if (showFeatureVals)
          Seq("features" -> JObject(List(updateFreq, popularity).flatten.toMap))
        else Seq.empty

        // TODO: When production mapping changes domain_cname back to array,
        // and add back .last.asInstanceOf[JArray]
        val cname: String = json.dyn.socrata_id.domain_cname.! match {
          case JString(string) => string
          case JArray(elems) => elems.last.asInstanceOf[JString].string
          case jv: JValue => throw new NoSuchElementException(s"Unexpected json value $jv")
        }

        val datasetID = json.dyn.socrata_id.dataset_id.!.asInstanceOf[JString]

        val pageID = json.dyn.socrata_id.page_id.?

        val link = pageID match {
          case Right(pgId) =>
            JString(s"""https://${cname}/view/${pgId.asInstanceOf[JString].string}""")
          case _ =>
            JString(s"""https://${cname}/d/${datasetID.string}""")
        }

        val customerCategory = json.dyn.customer_category.? match {
          case Left(e) => None
          case Right(jv) if jv != JNull => Some(jv)
          case Right(jv) => Some(JString(""))
        }

        SearchResult(
          json.dyn.resource.!,
          Classification(categories, tags, customerCategory),
          Map("domain" -> JString(cname)) ++ score ++ featureVals,
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
          params.searchContext,
          params.categories,
          params.tags,
          params.only,
          params.boosts,
          params.minShouldMatch,
          params.slop,
          params.functionScores,
          params.offset,
          params.limit
        )

        logger.info("ElasticSearch Query using Java Client API:\n" + request.internalBuilder());

        val response = Try(request.execute().actionGet())

        response match {
          case Success(res) =>
            val timings = InternalTimings(Timings.elapsedInMillis(now), Option(res.getTookInMillis()))
            val count = res.getHits().getTotalHits()
            val formattedResults = format(params.showFeatureVals, params.showScore, res)
              .copy(resultSetSize = Some(count), timings = Some(timings))

            val logMsg = LogHelper.formatRequest(req, timings)
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
