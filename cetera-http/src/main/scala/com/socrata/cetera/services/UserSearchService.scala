package com.socrata.cetera.services

import scala.util.control.NonFatal

import com.socrata.http.server._
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource
import org.slf4j.LoggerFactory

import com.socrata.cetera._
import com.socrata.cetera.auth.CoreClient
import com.socrata.cetera.handlers.util._
import com.socrata.cetera.response.JsonResponses._
import com.socrata.cetera.response.{InternalTimings, SearchResults, Timings, _}
import com.socrata.cetera.search.{DomainClient, UserClient}
import com.socrata.cetera.types._
import com.socrata.cetera.util.{ElasticsearchError, LogHelper}

class UserSearchService(userClient: UserClient, coreClient: CoreClient, domainClient: DomainClient) {
  lazy val logger = LoggerFactory.getLogger(getClass)

  // Hi, my name is Werner Brandes. My voice is my passport, verify me.
  def verifyUserAuthorization(
      cookie: Option[String],
      domainCname: String,
      requestId: Option[String])
  : (Boolean, Seq[String]) = {
    cookie match {
      case Some(c) =>
        val (authUser, setCookies) = coreClient.fetchUserByCookie(domainCname, c, requestId)
        (authUser.exists(_.canViewUsers), setCookies)
      case _ =>
        (false, Seq.empty[String])
    }
  }

  /** Gets the search context domains and performs authentication/authorization on the logged-in user.
    *
    * @param domainCname the search context (customer domain)
    * @param cookie the currently logged-in user's core cookie
    * @param requestId a somewhat unique identifier that helps string requests together across services
    * @return (search context domain, elasticsearch timing, whether user is authorized, client cookies to set)
    */
  // TODO: clean this up to separate the two or more things this function performs
  def fetchDomainAndUserAuthorization(
      domainCname: Option[String],
      cookie: Option[String],
      requestId: Option[String])
  : (Option[Domain], Long, Boolean, Seq[String]) =
    domainCname.map { extendedHost =>
      val (domainFound, domainTime) = domainClient.find(extendedHost)
      domainFound.map { domain: Domain =>
        val (authorized, setCookies) = verifyUserAuthorization(cookie, domain.domainCname, requestId)
        (domainFound, domainTime, authorized, setCookies)
      }.getOrElse((None, domainTime, false, Seq.empty[String]))
    }.getOrElse((None, 0L, false, Seq.empty[String]))

  def doSearch(
      queryParameters: MultiQueryParams,
      cookie: Option[String],
      extendedHost: Option[String],
      requestId: Option[String])
  : (StatusResponse, SearchResults[DomainUser], InternalTimings, Seq[String]) = {
    val now = Timings.now()

    val (domain, domainSearchTime, authorized, setCookies) =
      fetchDomainAndUserAuthorization(extendedHost, cookie, requestId)

    val (status, results: SearchResults[DomainUser], timings) =
      if (authorized) {
        val query = queryParameters.getOrElse("q", Seq.empty).headOption
        val (results: Set[EsUser], userSearchTime) = userClient.search(query)

        val formattedResults = SearchResults[DomainUser](results.toSeq.flatMap(u => DomainUser(domain, u)),
          results.size)
        val timings = InternalTimings(Timings.elapsedInMillis(now), Seq(domainSearchTime, userSearchTime))

        (OK, formattedResults.copy(timings = Some(timings)), timings)
      } else {
        logger.warn(s"user failed authorization $cookie $extendedHost $requestId")
        (Unauthorized, SearchResults(Seq.empty, 0),
          InternalTimings(Timings.elapsedInMillis(now), Seq(domainSearchTime)))
      }

    (status, results, timings, setCookies)
  }

  // $COVERAGE-OFF$ jetty wiring
  def search(req: HttpRequest): HttpResponse = {
    logger.debug(LogHelper.formatHttpRequestVerbose(req))

    val cookie = req.header(HeaderCookieKey)
    val extendedHost = req.header(HeaderXSocrataHostKey)
    val requestId = req.header(HeaderXSocrataRequestIdKey)

    try {
      val (status, results, timings, setCookies) = doSearch(req.multiQueryParams, cookie, extendedHost, requestId)
      logger.info(LogHelper.formatRequest(req, timings))
      Http.decorate(Json(results, pretty = true), status, setCookies)
    } catch {
      case e: IllegalArgumentException =>
        logger.info(e.getMessage)
        BadRequest ~> HeaderAclAllowOriginAll ~> jsonError(e.getMessage)
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
