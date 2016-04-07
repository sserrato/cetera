package com.socrata.cetera.authentication

import scala.util.control.NonFatal

import com.rojoma.simplearm.v2.{ResourceScope, using}
import com.socrata.http.client.{RequestBuilder, HttpClientHttpClient}
import org.apache.http.HttpStatus._
import org.slf4j.LoggerFactory

class CoreClient(httpClient: HttpClientHttpClient, host: String,
                 port: Int, connectTimeoutMs: Int, appToken: String) {

  val logger = LoggerFactory.getLogger(getClass)
  val coreBaseRequest = RequestBuilder(host, secure = false)
    .port(port)
    .connectTimeoutMS(connectTimeoutMs)
    .addHeader(("Content-Type", "application/json; charset=utf-8"))
    .addHeader(("X-App-Token", appToken))

  def fetchUserByCookie(domain: String, cookie: String): Option[User] = {
    if (cookie.nonEmpty) {
      val req = coreBaseRequest
        .addPath("users.json")
        .addParameters(Map("method" -> "getCurrent"))
        .addHeader(("X-Socrata-Host", domain))
        .addHeader(("Cookie", cookie))

      logger.info(s"Validating user on domain $domain")
      using(new ResourceScope(s"retrieving current user from core")) { rs =>
        try {
          val res = httpClient.execute(req.get, rs)
          res.resultCode match {
            case SC_OK => res.value[User]() match {
              case Right(u) => Some(u)
              case Left(err) =>
                logger.error(s"Could not parse core user data: \n ${err.english}")
                None
            }
            case SC_FORBIDDEN | SC_UNAUTHORIZED =>
              logger.warn(s"User is unauthorized on $domain.")
              None
            case code: Int =>
              logger.warn(s"Could not validate cookie with core. Core returned a $code.")
              None
          }
        } catch {
          case NonFatal(e) =>
            logger.error("Cannot reach core to validate cookie")
            None
        }
      }
    } else {
      None
    }
  }

  def fetchUserById(domain: String, id: String): Option[User] = {
    val req = coreBaseRequest
      .path(List("users", id))
      .addHeader(("X-Socrata-Host", domain))

    logger.info(s"Looking up user $id on domain $domain")
    using(new ResourceScope(s"retrieving user from core")) { rs =>
      try {
        val res = httpClient.execute(req.get, rs)
        res.resultCode match {
          case SC_OK => res.value[User]() match {
            case Right(u) => Some(u)
            case Left(err) =>
              logger.error(s"Could not parse core data for user $id: \n ${err.english}")
              None
          }
          case code: Int =>
            logger.warn(s"Could not fetch user $id from core. Core returned a $code.")
            None
        }
      } catch {
        case NonFatal(e) =>
          logger.error(s"Cannot reach core to fetch user $id")
          None
      }
    }
  }
}

