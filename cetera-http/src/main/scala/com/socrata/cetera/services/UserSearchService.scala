package com.socrata.cetera.services

import scala.util.control.NonFatal

import com.socrata.http.server._
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource
import org.slf4j.LoggerFactory

import com.socrata.cetera._
import com.socrata.cetera.search.UserClient
import com.socrata.cetera.types.User
import com.socrata.cetera.util.JsonResponses._
import com.socrata.cetera.util.{InternalTimings, SearchResults, Timings, _}

class UserSearchService(userClient: UserClient) {
  lazy val logger = LoggerFactory.getLogger(getClass)

  def doSearch(
      queryParameters: MultiQueryParams,
      cookie: Option[String],
      extendedHost: Option[String],
      requestId: Option[String])
  : (SearchResults[User], InternalTimings, Seq[String]) = {
    val now = Timings.now()

    val query: Option[String] = queryParameters.getOrElse("q", Seq.empty).headOption
    val (results, timing) = userClient.search(query)

    val formattedResults: SearchResults[User] = new SearchResults[User](results.toSeq)
    val timings = InternalTimings(Timings.elapsedInMillis(now), Seq(timing))

    (formattedResults.copy(resultSetSize = Some(results.size), timings = Some(timings)), timings)
  }

  // $COVERAGE-OFF$ jetty wiring
  def search(req: HttpRequest): HttpResponse = {
    logger.debug(LogHelper.formatHttpRequestVerbose(req))

    val cookie = req.header(HeaderCookieKey)
    val extendedHost = req.header(HeaderXSocrataHostKey)
    val requestId = req.header(HeaderXSocrataRequestIdKey)

    try {
      val (formattedResults, timings) = doSearch(req.multiQueryParams, cookie, extendedHost, requestId)
      logger.info(LogHelper.formatRequest(req, timings))
      Http.decorate(Json(formattedResults, pretty = true), OK, Seq.empty)
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
