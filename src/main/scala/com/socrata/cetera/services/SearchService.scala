package com.socrata.cetera.services

import javax.servlet.http.HttpServletResponse

import com.rojoma.json.v3.ast._
import com.rojoma.json.v3.codec.DecodeError
import com.rojoma.json.v3.io.JsonReader
import com.rojoma.json.v3.jpath.JPath
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, JsonKeyStrategy, Strategy}
import com.socrata.cetera._
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

@JsonKeyStrategy(Strategy.Underscore)
case class Classification(categories: Seq[JValue],
                          tags: Seq[JValue],
                          domainCategory: Option[JValue],
                          domainTags: Option[JValue],
                          domainMetadata: Option[JValue])

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

class SearchService(elasticSearchClient: Option[ElasticSearchClient]) extends SimpleResource {
  lazy val logger = LoggerFactory.getLogger(classOf[SearchService])

  // TODO: cetera-etl rename customer_blah to domain_blah
  private def domainCategory(j: JValue): Option[JValue] = j.dyn.customer_category.? match {
    case Left(e) => None
    case Right(jv) => Some(jv)
  }

  private def domainTags(j: JValue): Option[JValue] = j.dyn.customer_tags.? match {
    case Left(e) => None
    case Right(jv) => Some(jv)
  }

  private def domainMetadata(j: JValue): Option[JValue] = j.dyn.customer_metadata_flattened.? match {
    case Left(e) => None
    case Right(jv) => Some(jv)
  }

  private def categories(j: JValue): Stream[JValue] =
    new JPath(j).down("animl_annotations").down("categories").*.down("name").finish.distinct

  private def tags(j: JValue): Stream[JValue] =
    new JPath(j).down("animl_annotations").down("tags").*.down("name").finish.distinct

  private def popularity(j: JValue): Option[(String,JValue)] =
    j.dyn.popularity.?.fold(_ => None, r => Option(("popularity", r)))

  private def updateFreq(j: JValue): Option[(String,JValue)] =
    j.dyn.update_freq.?.fold(_ => None, r => Option(("update_freq", r)))

  // TODO: When production mapping changes domain_cname back to array, and add back .last.asInstanceOf[JArray]
  private def cname(j: JValue): String = j.dyn.socrata_id.domain_cname.! match {
    case JString(string) => string
    case JArray(elems) => elems.last.asInstanceOf[JString].string
    case jv: JValue => throw new NoSuchElementException(s"Unexpected json value $jv")
  }

  private def link(cname: String, pageId: Either[DecodeError.Simple,JValue], datasetId: JString): JString =
    pageId match {
      case Right(jv) => JString( s"""https://$cname/view/${jv.asInstanceOf[JString].string}""")
      case _         => JString( s"""https://$cname/d/${datasetId.string}""")
    }

  def format(showFeatureVals: Boolean,
             showScore: Boolean,
             searchResponse: SearchResponse): SearchResults[SearchResult] = {
    SearchResults(searchResponse.getHits.hits().map { hit =>
      val json = JsonReader.fromString(hit.sourceAsString())

      val score = if (showScore) Seq("score" -> JNumber(hit.score)) else Seq.empty

      val featureVals = if (showFeatureVals) {
        Seq("features" -> JObject(List(updateFreq(json), popularity(json)).flatten.toMap))
      } else {
        Seq.empty
      }

      SearchResult(
        json.dyn.resource.!,
        Classification(
          categories(json),
          tags(json),
          domainCategory(json),
          domainTags(json),
          domainMetadata(json)),
        Map("domain" -> JString(cname(json))) ++ score ++ featureVals,
        link(cname(json), json.dyn.socrata_id.page_id.?, json.dyn.socrata_id.dataset_id.!.asInstanceOf[JString])
      )
    })
  }

  def search(req: HttpRequest): HttpServletResponse => Unit = {
    val now = Timings.now()

    QueryParametersParser(req) match {
      case Right(params) =>
        val request = elasticSearchClient.getOrElse(throw new NullPointerException).buildSearchRequest(
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

        logger.info("ElasticSearch query: " + request.internalBuilder().toString.replaceAll("""[\n\s]+""", " "))

        val response = Try(request.execute().actionGet())

        response match {
          case Success(res) =>
            val timings = InternalTimings(Timings.elapsedInMillis(now), Option(res.getTookInMillis))
            val count = res.getHits.getTotalHits
            val formattedResults = format(params.showFeatureVals, params.showScore, res)
              .copy(resultSetSize = Some(count), timings = Some(timings))

            val logMsg = LogHelper.formatRequest(req, timings)
            logger.info(logMsg)

            val payload = Json(formattedResults, pretty=true)
            OK ~> HeaderAclAllowOriginAll ~> payload

          case Failure(ex) =>
            InternalServerError ~> HeaderAclAllowOriginAll ~> jsonError(s"Database error: ${ex.getMessage}")
        }

      case Left(errors) =>
        val msg = errors.map(_.message).mkString(", ")
        BadRequest ~> HeaderAclAllowOriginAll ~> jsonError(s"Invalid query parameters: $msg")
    }
  }

  object Service extends SimpleResource {
    override def get: HttpService = search
  }
}
