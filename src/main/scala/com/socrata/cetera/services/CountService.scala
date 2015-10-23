package com.socrata.cetera.services

import javax.servlet.http.HttpServletResponse

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.codec.DecodeError
import com.rojoma.json.v3.io.JsonReader
import com.rojoma.json.v3.matcher.{FirstOf, PObject, Variable}
import com.socrata.cetera._
import com.socrata.cetera.search.ElasticSearchClient
import com.socrata.cetera.types._
import com.socrata.cetera.util.JsonResponses._
import com.socrata.cetera.util._
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.{HttpRequest, HttpService}
import org.slf4j.LoggerFactory

class CountService(elasticSearchClient: Option[ElasticSearchClient]) {
  lazy val logger = LoggerFactory.getLogger(classOf[CountService])

  // Possibly belongs in the client
  def extract(body: JValue): Either[DecodeError, Seq[JValue]]= {
    val buckets = Variable.decodeOnly[Seq[JValue]]
    val pattern = PObject(
      "aggregations" -> FirstOf(
        PObject("domains" -> PObject("buckets" -> buckets)),
        PObject("annotations" -> PObject("names" -> PObject("buckets" -> buckets)))))

    pattern.matches(body).right.map(buckets)
  }

  // Unhandled exception on missing key
  def format(counts: Seq[JValue]): SearchResults[Count] =  {
    SearchResults(
      counts.map { c => Count(c.dyn.key.!, c.dyn.doc_count.!) }
    )
  }

  def aggregate(field: CeteraFieldType with Countable with Rawable)(req: HttpRequest): HttpServletResponse => Unit = {
    val now = Timings.now()

    implicit val cEncode = field match {
      case DomainFieldType => Count.encode("domain")
      case CategoriesFieldType => Count.encode("category")
      case TagsFieldType => Count.encode("tag")
    }

    QueryParametersParser(req) match {
      case Left(errors) =>
        val msg = errors.map(_.message).mkString(", ")
        BadRequest ~> HeaderAclAllowOriginAll ~> jsonError(s"Invalid query parameters: $msg")
      case Right(params) =>
        val request = elasticSearchClient.getOrElse(throw new NullPointerException).buildCountRequest(
          field,
          params.searchQuery,
          params.domains,
          params.searchContext,
          params.categories,
          params.tags,
          params.only
        )

        try {
          val res = request.execute().actionGet()
          val timings = InternalTimings(Timings.elapsedInMillis(now), Option(res.getTookInMillis))

          val json = JsonReader.fromString(res.toString)
          val counts = extract(json) match {
            case Right(extracted) => extracted
            case Left(error) =>
              logger.error(error.english)
              throw new Exception("error!  ERROR!! DOES NOT COMPUTE")
          }
          val formattedResults = format(counts).copy(timings = Some(timings))

          val logMsg = LogHelper.formatRequest(req, timings)
          logger.info(logMsg)

          val payload = Json(formattedResults, pretty = true)
          OK ~> HeaderAclAllowOriginAll ~> payload
        } catch {
          case e: Exception =>
            val esError = ElasticsearchError(e)
            logger.error("Database error: ${esError.getMessage}")
            InternalServerError ~> HeaderAclAllowOriginAll ~> jsonError(s"Database error", esError)
        }
    }
  }

  case class Service(field: CeteraFieldType with Countable with Rawable) extends SimpleResource {
    override def get: HttpService = aggregate(field)
  }
}
