package com.socrata.cetera.services

import scala.util.control.NonFatal

import com.socrata.http.server._
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource
import org.slf4j.LoggerFactory

import com.socrata.cetera._
import com.socrata.cetera.auth.{AuthParams, CoreClient}
import com.socrata.cetera.errors.{DomainNotFoundError, ElasticsearchError, UnauthorizedError}
import com.socrata.cetera.handlers.QueryParametersParser
import com.socrata.cetera.handlers.util._
import com.socrata.cetera.response.JsonResponses.jsonError
import com.socrata.cetera.response.SearchResults._
import com.socrata.cetera.response.{Http, InternalTimings, SearchResults, Timings}
import com.socrata.cetera.search.{DomainClient, UserClient}
import com.socrata.cetera.types.DomainUser
import com.socrata.cetera.util.LogHelper

class UserSearchService(userClient: UserClient, domainClient: DomainClient, coreClient: CoreClient) {
  lazy val logger = LoggerFactory.getLogger(getClass)

  def doSearch(
      queryParameters: MultiQueryParams,
      authParams: AuthParams,
      extendedHost: Option[String],
      requestId: Option[String])
    : (StatusResponse, SearchResults[DomainUser], InternalTimings, Seq[String]) = {

    val now = Timings.now()
    val (authorizedUser, setCookies) = coreClient.optionallyAuthenticateUser(extendedHost, authParams, requestId)

    if (authorizedUser.isEmpty) {
      throw UnauthorizedError(authorizedUser, "search users")
    } else {
      val params = QueryParametersParser.prepUserParams(queryParameters)
      val searchParams = params.searchParamSet
      val pagingParams = params.pagingParamSet
      val (domain, domSearchTime) = searchParams.domain match {
        case None => (None, 0L)
        case Some(d) => domainClient.find(d)
      }
      val (domainForRoles, domainSearchTime) = (domain, extendedHost) match {
        case (None, Some(host)) => domainClient.find(host)
        case (_, _) => (domain, domSearchTime)
      }

      if (searchParams.domain.isDefined && domain.isEmpty){
        throw DomainNotFoundError(searchParams.domain.get)
      } else {
        val authedUser = authorizedUser.map(u => u.copy(authenticatingDomain = domainForRoles))
        val (users, totalCount, userSearchTime) =
          userClient.search(searchParams, pagingParams, domain.map(_.domainId), authedUser)

        val formattedResults = SearchResults(users.flatMap(u => DomainUser(domainForRoles, u)), totalCount)
        val timings = InternalTimings(Timings.elapsedInMillis(now), Seq(domainSearchTime, userSearchTime))
        (
          OK,
          formattedResults.copy(timings = Some(timings)),
          timings,
          setCookies
          )
      }
    }
  }

  // $COVERAGE-OFF$ jetty wiring
  def search(req: HttpRequest): HttpResponse = {
    logger.debug(LogHelper.formatHttpRequestVerbose(req))

    val authParams = AuthParams.fromHttpRequest(req)
    val extendedHost = req.header(HeaderXSocrataHostKey)
    val requestId = req.header(HeaderXSocrataRequestIdKey)

    try {
      val (status, results, timings, setCookies) = doSearch(req.multiQueryParams, authParams, extendedHost, requestId)
      logger.info(LogHelper.formatRequest(req, timings))
      Http.decorate(Json(results, pretty = true), status, setCookies)
    } catch {
      case e: IllegalArgumentException =>
        logger.info(e.getMessage)
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

  object Service extends SimpleResource {
    override def get: HttpService = search
  }
  // $COVERAGE-ON$
}
