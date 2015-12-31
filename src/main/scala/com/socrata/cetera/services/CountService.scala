package com.socrata.cetera.services

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.codec.DecodeError
import com.rojoma.json.v3.io.JsonReader
import com.rojoma.json.v3.matcher.{FirstOf, PObject, Variable}
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.{HttpRequest, HttpResponse, HttpService}
import org.slf4j.LoggerFactory

import com.socrata.cetera._
import com.socrata.cetera.search.{DocumentClient, DomainClient}
import com.socrata.cetera.types._
import com.socrata.cetera.util.JsonResponses._
import com.socrata.cetera.util._

class CountService(documentClient: DocumentClient, domainClient: DomainClient) {
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
  def format(counts: Seq[JValue]): SearchResults[Count] =
    SearchResults(counts.map { c => Count(c.dyn.key.!, c.dyn.doc_count.!) })

  def doAggregate(field: CeteraFieldType with Countable with Rawable,
                  queryParameters: MultiQueryParams): (SearchResults[Count], InternalTimings) = {
    val now = Timings.now()

    QueryParametersParser(queryParameters) match {
      case Left(errors) =>
        val msg = errors.map(_.message).mkString(", ")
        throw new IllegalArgumentException(s"Invalid query parameters: $msg")

      case Right(params) =>
        val domain = params.searchContext.flatMap(domainClient.getDomain)
        val res = documentClient.buildCountRequest(
          field,
          params.searchQuery,
          params.domains,
          domain,
          params.categories,
          params.tags,
          params.only
        ).execute.actionGet
        val timings = InternalTimings(Timings.elapsedInMillis(now), Option(res.getTookInMillis))
        val json = JsonReader.fromString(res.toString)
        val counts = extract(json) match {
          case Right(extracted) => extracted
          case Left(error) =>
            logger.error(error.english)
            throw new Exception("error!  ERROR!! DOES NOT COMPUTE")
        }
        val formattedResults: SearchResults[Count] = format(counts).copy(timings = Some(timings))
        (formattedResults, timings)
    }
  }

  // $COVERAGE-OFF$ jetty wiring
  def aggregate(field: CeteraFieldType with Countable with Rawable)(req: HttpRequest): HttpResponse = {
    implicit val cEncode = field match {
      case DomainFieldType => Count.encode("domain")
      case CategoriesFieldType => Count.encode("category")
      case TagsFieldType => Count.encode("tag")
    }

    try {
      val (formattedResults, timings) = doAggregate(field, req.multiQueryParams)
      logger.info(LogHelper.formatRequest(req, timings))
      OK ~> HeaderAclAllowOriginAll ~> Json(formattedResults, pretty = true)
    } catch {
      case e: IllegalArgumentException =>
        logger.info(e.getMessage)
        BadRequest ~> HeaderAclAllowOriginAll ~> jsonError(e.getMessage)
      case e: Exception =>
        val esError = ElasticsearchError(e)
        logger.error(s"Database error: ${esError.getMessage}")
        InternalServerError ~> HeaderAclAllowOriginAll ~> jsonError(s"Database error", esError)
    }
  }

  case class Service(field: CeteraFieldType with Countable with Rawable) extends SimpleResource {
    override def get: HttpService = aggregate(field)
  }
  // $COVERAGE-ON$
}
