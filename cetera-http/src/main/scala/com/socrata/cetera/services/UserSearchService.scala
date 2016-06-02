package com.socrata.cetera.services

import scala.util.control.NonFatal

import com.socrata.http.server._
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource
import org.slf4j.LoggerFactory

import com.socrata.cetera._
import com.socrata.cetera.authentication.{CoreClient, User => AuthUser}
import com.socrata.cetera.search.UserClient
import com.socrata.cetera.types.User
import com.socrata.cetera.util.JsonResponses._
import com.socrata.cetera.util.{InternalTimings, SearchResults, Timings, _}

class UserSearchService(userClient: UserClient, coreClient: CoreClient) {
  lazy val logger = LoggerFactory.getLogger(getClass)

  // Hi, my name is Werner Brandes. My voice is my passport, verify me.
  def verifyUserAuthorization(
      cookie: Option[String],
      extendedHost: Option[String],
      requestId: Option[String])
  : (Boolean, Seq[String]) = {
    (cookie, extendedHost) match {
      case (Some(c), Some(h)) =>
        val (authUser, setCookies) = coreClient.fetchUserByCookie(h, c, requestId)
        (authUser.exists(_.canViewUsers), setCookies)
      case _ =>
        (false, Seq.empty[String])
    }
  }

  def doSearch(
      queryParameters: MultiQueryParams,
      cookie: Option[String],
      extendedHost: Option[String],
      requestId: Option[String])
  : (StatusResponse, SearchResults[User], InternalTimings, Seq[String]) = {
    val now = Timings.now()

    val (authorized, setCookies) = verifyUserAuthorization(cookie, extendedHost, requestId)

    val (status, results: SearchResults[User], timings) =
      if (authorized) {
        val query = queryParameters.getOrElse("q", Seq.empty).headOption
        val (results, timing) = userClient.search(query)

        val formattedResults = new SearchResults[User](results.toSeq)
        val timings = InternalTimings(Timings.elapsedInMillis(now), Seq(timing))

        (OK, formattedResults.copy(resultSetSize = Some(results.size), timings = Some(timings)), timings)
      } else {
        logger.warn(s"user failed authorization $cookie $extendedHost $requestId")
        (Unauthorized, new SearchResults(Seq.empty), InternalTimings(Timings.elapsedInMillis(now), Seq.empty))
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
