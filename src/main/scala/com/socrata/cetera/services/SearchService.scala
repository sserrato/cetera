package com.socrata.cetera.services

import javax.servlet.http.HttpServletResponse

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.io.JsonReader
import com.rojoma.json.v3.jpath.JPath
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.{HttpRequest, HttpResponse, HttpService}
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchResponse}
import org.elasticsearch.client.Client
import org.elasticsearch.index.query.{FilterBuilders, QueryBuilders}
import org.slf4j.LoggerFactory

class SearchService(client: Client) extends SimpleResource {
  lazy val logger = LoggerFactory.getLogger(classOf[SearchService])

  // Assumes query extraction and validation have already been done
  def buildSearchRequest(searchQuery: Option[String] = None,
                         domains: Option[Set[String]] = None,
                         categories: Option[Set[String]] = None,
                         only: Option[String] = None,
                         offset: Int,
                         limit: Int): SearchRequestBuilder = {

    val filteredQuery = {
      val matchQuery = searchQuery match {
        case None => QueryBuilders.matchAllQuery()
        case Some(sq) => QueryBuilders.matchQuery("_all", sq)
      }

      // Effectively an OR of domain filters
      val domainFilter = domains match {
        case None => FilterBuilders.matchAllFilter()
        case Some(d) => FilterBuilders.termsFilter("domain_cname_exact", d.toSeq:_*)
      }

      // Effectively an OR of category filters
      val categoryFilter = categories match {
        case None => FilterBuilders.matchAllFilter()
        case Some(c) => FilterBuilders.termsFilter("categories", c.toSeq:_*)
      }

      // CNF -- an AND of ORs
      val domainAndCategoryFilter =
        FilterBuilders.andFilter(
          domainFilter,
          categoryFilter
        )

      QueryBuilders.filteredQuery(matchQuery, domainAndCategoryFilter)
    }

    // Imperative-style builder function
    client.prepareSearch()
      .setTypes(only.toList:_*)
      .setQuery(filteredQuery)
      .setFrom(offset)
      .setSize(limit)
  }

  // Fails silently if path does not exist
  def extractResources(body: JValue): Stream[JValue] = {
    val jPath = new JPath(body)
    jPath.down("hits").down("hits").*.down("_source").down("resource").finish
  }

  def formatSearchResults(searchResponse: SearchResponse): Map[String, Stream[Map[String, JValue]]] = {
    val body = JsonReader.fromString(searchResponse.toString)
    val resources = extractResources(body)
    Map("results" -> resources.map { r => Map("resource" -> r) })
  }


  //////////////////////////
  // belongs in socrata-http
  //
  sealed trait ParamConversionFailure
  case class InvalidValue(v: String) extends ParamConversionFailure

  trait ParamConverter[T] {
    def convertFrom(s: String): Either[ParamConversionFailure, T]
  }

  implicit class TypedQueryParams(req: HttpRequest) {
    def queryParamOrElse[T: ParamConverter](name: String, default: T): Either[ParamConversionFailure, T] = {
      req.queryParameters
        .get(name)
        .map(implicitly[ParamConverter[T]].convertFrom)
        .getOrElse(Right(default))
    }
  }

  def validated[T](x: Either[ParamConversionFailure, T]): T = x match {
    case Right(v) => v
    case Left(_) => throw new Exception("Parameter validation failure")
  }

  object ParamConverter {
    implicit object IntParam extends ParamConverter[Int] {
      override def convertFrom(s: String): Either[ParamConversionFailure, Int] = {
        try { Right(s.toInt) }
        catch { case e : Exception => Left(InvalidValue(s)) }
      }
    }

    def filtered[A : ParamConverter, B](f: A => Option[B]): ParamConverter[B] = {
      new ParamConverter[B] {
        def convertFrom(s: String): Either[ParamConversionFailure, B] = {
          implicitly[ParamConverter[A]].convertFrom(s) match {
            case Right(a) =>
              f(a) match {
                case Some(b) => Right(b)
                case None => Left(InvalidValue(s))
              }
            case Left(e) => Left(e)
          }
        }
      }
    }
  }
  //
  ////////////////////////////


  case class NonNegativeInt(value: Int)
  implicit val nonNegativeIntParamConverter = ParamConverter.filtered { (a: Int) =>
    if (a >= 0)
      Some(NonNegativeInt(a))
    else
      None
  }

  // Failure cases are not handled, in particular actionGet() from ES throws
  def search(req: HttpRequest): HttpServletResponse => Unit = {
    val logMsg = List[String]("[" + req.servletRequest.getMethod + "]",
      req.requestPathStr,
      req.queryStr.getOrElse("<no query params>"),
      "requested by",
      req.servletRequest.getRemoteHost).mkString(" -- ")
    logger.info(logMsg)

    val searchRequest = buildSearchRequest(
      searchQuery = req.queryParameters.get("q"),
      domains = req.queryParameters.get("domains").map(_.split(",").toSet),
      categories = req.queryParameters.get("categories").map(_.split(",").toSet),
      only = req.queryParameters.get("only"),
      offset = validated(req.queryParamOrElse("offset", NonNegativeInt(0))).value,
      limit = validated(req.queryParamOrElse("limit", NonNegativeInt(100))).value
    )
    val searchResponse = searchRequest.execute().actionGet()

    val formattedResults = formatSearchResults(searchResponse)
    val payload = Json(formattedResults, pretty=true)

    OK ~> payload
  }

  override def get: HttpService = search
}
