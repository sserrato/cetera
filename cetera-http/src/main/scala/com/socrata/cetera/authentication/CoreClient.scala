package com.socrata.cetera.authentication

import scala.util.control.NonFatal

import com.rojoma.simplearm.v2.{ResourceScope, using}
import com.socrata.http.client.{RequestBuilder, HttpClientHttpClient}
import org.apache.http.HttpStatus._
import org.slf4j.LoggerFactory

class CoreClient(httpClient: HttpClientHttpClient, host: String,
                 port: Int, connectTimeoutMs: Int, appToken: String) {

  val logger = LoggerFactory.getLogger(getClass)
  val coreBaseRequest = RequestBuilder(host, secure = false).port(port).connectTimeoutMS(connectTimeoutMs)

  def fetchCurrentUser(domain: String, cookie: Option[String]): Option[User] = {
    cookie match {
      case Some(c) if c.nonEmpty =>
        val req = coreBaseRequest
          .addPath("users.json")
          .addParameters(Map("method" -> "getCurrent"))
          .addHeader(("Content-Type", "application/json; charset=utf-8"))
          .addHeader(("X-App-Token", appToken))
          .addHeader(("X-Socrata-Host", domain))
          .addHeader(("Cookie", c))

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
              case SC_FORBIDDEN | SC_UNAUTHORIZED => None
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
      case _ => None
    }
  }
}

