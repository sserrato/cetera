package com.socrata.cetera.services

import scala.util.control.NonFatal

import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.{HttpRequest, HttpResponse, HttpService}
import org.slf4j.LoggerFactory

import com.socrata.cetera._
import com.socrata.cetera.auth.{AuthParams, VerificationClient}
import com.socrata.cetera.errors.{DomainNotFoundError, ElasticsearchError, UnauthorizedError}
import com.socrata.cetera.handlers._
import com.socrata.cetera.handlers.ParamValidator
import com.socrata.cetera.handlers.util._
import com.socrata.cetera.metrics.BalboaClient
import com.socrata.cetera.response.JsonResponses._
import com.socrata.cetera.response._
import com.socrata.cetera.response.SearchResults._
import com.socrata.cetera.search.{BaseDocumentClient, BaseDomainClient, Visibility}
import com.socrata.cetera.types._
import com.socrata.cetera.util.LogHelper

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
      queryParameters: MultiQueryParams,
      visibility: Visibility,
      authParams: AuthParams,
      extendedHost: Option[String],
      requestId: Option[String])
    : (StatusResponse, SearchResults[SearchResult], InternalTimings, Seq[String]) = {

    val now = Timings.now()
    val (authorizedUser, setCookies) =
      verificationClient.fetchUserAuthorization(extendedHost, authParams, requestId, _ => true)

    // If authentication is required (varies by endpoint, see Visibility) then respond OK only when a valid user is
    // authorized, otherwise respond HTTP/401 Unauthorized.
    if (authorizedUser.isEmpty && visibility.authenticationRequired) {
      throw UnauthorizedError(authorizedUser, "search the internal catalog")
    } else {
      QueryParametersParser(queryParameters, extendedHost) match {
        case Left(errors) =>
          val msg = errors.map(_.message).mkString(", ")
          throw new IllegalArgumentException(s"Invalid query parameters: $msg")

        case Right(ValidatedQueryParameters(searchParams, scoringParams, pagingParams, formatParams)) =>
          val authedUserId = authorizedUser.map(_.id)
          val paramValidator = ParamValidator(searchParams, authedUserId, visibility)
          if (!paramValidator.userParamsAuthorized) {
            throw UnauthorizedError(authorizedUser, "search another users shared files")
          } else {
            val (domains, domainSearchTime) = domainClient.findSearchableDomains(
              searchParams.searchContext, extendedHost, searchParams.domains,
              excludeLockedDomains = true, authorizedUser, requestId
            )
            val domainSet = domains.addDomainBoosts(scoringParams.domainBoosts)
            val authedUser = authorizedUser.map(u => u.copy(authenticatingDomain = domainSet.extendedHost))

            val req = documentClient.buildSearchRequest(
              domainSet,
              searchParams, scoringParams, pagingParams,
              authedUser, visibility
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
  }

  def search(visibility: Visibility)(req: HttpRequest): HttpResponse = {
    logger.debug(LogHelper.formatHttpRequestVerbose(req))

    val authParams = AuthParams.fromHttpRequest(req)
    val extendedHost = req.header(HeaderXSocrataHostKey)
    val requestId = req.header(HeaderXSocrataRequestIdKey)

    try {
      val (status, formattedResults, timings, setCookies) =
        doSearch(req.multiQueryParams, visibility, authParams, extendedHost, requestId)

      logger.info(LogHelper.formatRequest(req, timings))
      Http.decorate(Json(formattedResults, pretty = true), status, setCookies)
    } catch {
      case e: IllegalArgumentException =>
        logger.error(e.getMessage)
        BadRequest ~> HeaderAclAllowOriginAll ~> jsonError(e.getMessage)
      case e: DomainNotFoundError =>
        logger.error(e.getMessage)
        NotFound ~> HeaderAclAllowOriginAll ~> jsonError(e.getMessage)
      case e: UnauthorizedError =>
        logger.error(e.getMessage)
        Unauthorized ~> HeaderAclAllowOriginAll ~> jsonError(e.getMessage)
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
