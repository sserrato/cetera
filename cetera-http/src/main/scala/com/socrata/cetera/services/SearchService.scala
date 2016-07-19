package com.socrata.cetera.services

import scala.util.control.NonFatal

import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.{HttpRequest, HttpResponse, HttpService}
import org.slf4j.LoggerFactory

import com.socrata.cetera._
import com.socrata.cetera.auth.VerificationClient
import com.socrata.cetera.handlers._
import com.socrata.cetera.handlers.util._
import com.socrata.cetera.metrics.BalboaClient
import com.socrata.cetera.response.JsonResponses._
import com.socrata.cetera.response._
import com.socrata.cetera.search.{BaseDocumentClient, BaseDomainClient, DomainNotFound, Visibility}
import com.socrata.cetera.types._
import com.socrata.cetera.util.{ElasticsearchError, LogHelper}

class SearchService(
    documentClient: BaseDocumentClient,
    domainClient: BaseDomainClient,
    balboaClient: BalboaClient,
    verificationClient: VerificationClient) extends SimpleResource {
  lazy val logger = LoggerFactory.getLogger(classOf[SearchService])

  def logSearchTerm(domain: Option[Domain], query: QueryType): Unit = {
    domain.foreach { d =>
      query match {
        case NoQuery => // nothing to log to balboa
        case SimpleQuery(q) => balboaClient.logQuery(d.domainId, q)
        case AdvancedQuery(q) => balboaClient.logQuery(d.domainId, q)
      }
    }
  }

  def doSearch(
      queryParameters: MultiQueryParams, // scalastyle:ignore parameter.number method.length
      visibility: Visibility,
      cookie: Option[String],
      extendedHost: Option[String],
      requestId: Option[String])
    : (StatusResponse, SearchResults[SearchResult], InternalTimings, Seq[String]) = {

    val now = Timings.now()

    val (authorizedUser, setCookies) =
      verificationClient.fetchUserAuthorization(extendedHost, cookie, requestId, _ => true)

    // If authentication is required (varies by endpoint, see Visibility) then respond OK only when a valid user is
    // authorized, otherwise respond HTTP/401 Unauthorized.
    if (authorizedUser.isEmpty && visibility.authenticationRequired) {
      (
        Unauthorized,
        SearchResults(Seq.empty[SearchResult], 0),
        InternalTimings(Timings.elapsedInMillis(now), Seq(0)),
        setCookies
      )
    } else {
      QueryParametersParser(queryParameters, extendedHost) match {
        case Left(errors) =>
          val msg = errors.map(_.message).mkString(", ")
          throw new IllegalArgumentException(s"Invalid query parameters: $msg")

        case Right(ValidatedQueryParameters(searchParams, scoringParams, pagingParams, formatParams)) =>
          val (domains, domainSearchTime) = domainClient.findSearchableDomains(
            searchParams.searchContext, searchParams.domains, excludeLockedDomains = true, authorizedUser, requestId
          )
          val domainSet = domains.addDomainBoosts(scoringParams.domainBoosts)

          val req = documentClient.buildSearchRequest(
            domainSet,
            searchParams, scoringParams, pagingParams,
            authorizedUser, visibility
          )
          logger.info(LogHelper.formatEsRequest(req))
          val res = req.execute.actionGet
          val formattedResults = Format.formatDocumentResponse(formatParams, domainSet, res)
          val timings = InternalTimings(Timings.elapsedInMillis(now), Seq(domainSearchTime, res.getTookInMillis))
          logSearchTerm(domainSet.searchContext, searchParams.searchQuery)

          (OK, formattedResults.copy(timings = Some(timings)), timings, setCookies)
      }
    }
  }

  def search(visibility: Visibility)(req: HttpRequest): HttpResponse = {
    logger.debug(LogHelper.formatHttpRequestVerbose(req))

    val cookie = req.header(HeaderCookieKey)
    val extendedHost = req.header(HeaderXSocrataHostKey)
    val requestId = req.header(HeaderXSocrataRequestIdKey)

    try {
      val (status, formattedResults, timings, setCookies) =
        doSearch(req.multiQueryParams, visibility, cookie, extendedHost, requestId)

      logger.info(LogHelper.formatRequest(req, timings))
      Http.decorate(Json(formattedResults, pretty = true), status, setCookies)
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
  case class Service(visibility: Visibility) extends SimpleResource {
    override def get: HttpService = search(visibility)
  }

  // $COVERAGE-ON$
}
