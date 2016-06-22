package com.socrata.cetera.services

import scala.util.control.NonFatal

import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.{HttpRequest, HttpResponse, HttpService}
import org.slf4j.LoggerFactory

import com.socrata.cetera._
import com.socrata.cetera.handlers.QueryParametersParser
import com.socrata.cetera.handlers.util._
import com.socrata.cetera.metrics.BalboaClient
import com.socrata.cetera.response.Format.formatDocumentResponse
import com.socrata.cetera.response.JsonResponses._
import com.socrata.cetera.response._
import com.socrata.cetera.search.{BaseDocumentClient, BaseDomainClient, DomainNotFound}
import com.socrata.cetera.types._
import com.socrata.cetera.util.{ElasticsearchError, LogHelper}

class SearchService(elasticSearchClient: BaseDocumentClient,
                    domainClient: BaseDomainClient,
                    balboaClient: BalboaClient) extends SimpleResource {
  lazy val logger = LoggerFactory.getLogger(classOf[SearchService])

  def logSearchTerm(domain: Option[Domain], query: QueryType): Unit = {
    domain.foreach(d =>
      query match {
        case NoQuery => // nothing to log to balboa
        case SimpleQuery(q) => balboaClient.logQuery(d.domainId, q)
        case AdvancedQuery(q) => balboaClient.logQuery(d.domainId, q)
      }
    )
  }


  def doSearch(
      queryParameters: MultiQueryParams, // scalastyle:ignore parameter.number method.length
      cookie: Option[String],
      extendedHost: Option[String],
      requestId: Option[String])
    : (SearchResults[SearchResult], InternalTimings, Seq[String]) = {

    val now = Timings.now()

    QueryParametersParser(queryParameters, extendedHost) match {
      case Left(errors) =>
        val msg = errors.map(_.message).mkString(", ")
        throw new IllegalArgumentException(s"Invalid query parameters: $msg")

      case Right(params) =>
        val (domainSet, domainSearchTime, setCookies) = domainClient.findSearchableDomains(
          params.searchContext, params.domains, excludeLockedDomains = true, cookie, requestId)

        val domainIdBoosts = domainSet.domainIdBoosts(params.domainBoosts)

        val req = elasticSearchClient.buildSearchRequest(
          params.searchQuery,
          domainSet,
          params.domainMetadata,
          params.categories,
          params.tags,
          params.datatypes,
          params.user,
          params.attribution,
          params.parentDatasetId,
          params.fieldBoosts,
          params.datatypeBoosts,
          domainIdBoosts,
          params.minShouldMatch,
          params.slop,
          params.offset,
          params.limit,
          params.sortOrder
        )

        logger.info(LogHelper.formatEsRequest(req))
        val res = req.execute.actionGet
        val formattedResults = formatDocumentResponse(domainSet.idCnameMap, params.showScore, res)
        val timings = InternalTimings(Timings.elapsedInMillis(now), Seq(domainSearchTime, res.getTookInMillis))
        logSearchTerm(domainSet.searchContext, params.searchQuery)

        (formattedResults.copy(timings = Some(timings)), timings, setCookies)
    }
  }

  def search(req: HttpRequest): HttpResponse = {
    logger.debug(LogHelper.formatHttpRequestVerbose(req))

    val cookie = req.header(HeaderCookieKey)
    val extendedHost = req.header(HeaderXSocrataHostKey)
    val requestId = req.header(HeaderXSocrataRequestIdKey)

    try {
      val (formattedResults, timings, setCookies) = doSearch(req.multiQueryParams, cookie, extendedHost, requestId)
      logger.info(LogHelper.formatRequest(req, timings))
      Http.decorate(Json(formattedResults, pretty = true), OK, setCookies)
    } catch {
      case e: IllegalArgumentException =>
        logger.info(e.getMessage)
        BadRequest ~> HeaderAclAllowOriginAll ~> jsonError(e.getMessage)
      case DomainNotFound(e) =>
        val msg = s"Domain not found: $e"
        logger.error(msg)
        NotFound ~> HeaderAclAllowOriginAll ~> jsonError(msg)
      case NonFatal(e) =>
        val msg = "Cetera search service error"
        val esError = ElasticsearchError(e)
        logger.error(s"$msg: $esError")
        InternalServerError ~> HeaderAclAllowOriginAll ~> jsonError("We're sorry. Something went wrong.")
    }
  }

  // $COVERAGE-OFF$ jetty wiring
  object Service extends SimpleResource {
    override def get: HttpService = search
  }
  // $COVERAGE-ON$
}
