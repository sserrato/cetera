package com.socrata.cetera.services

import com.socrata.http.server.implicits._
import com.socrata.http.server.responses.{Content, OK}
import com.socrata.http.server.routing.SimpleResource
import com.socrata.http.server.{HttpRequest, HttpService}
import org.slf4j.LoggerFactory

class VersionService
object VersionService {
  lazy val logger = LoggerFactory.getLogger(classOf[VersionService])
  lazy val version = buildinfo.BuildInfo

  // $COVERAGE-OFF$ jetty wiring
  object Service extends SimpleResource {
    override val get: HttpService = { (req: HttpRequest) =>
      logger.info(req.servletRequest.getRemoteHost + " is requesting the version number")
      logger.info(s"Reporting the version as: $version")
      OK ~> Content("application/json", version.toJson)
    }
  }
  // $COVERAGE-ON$
}
