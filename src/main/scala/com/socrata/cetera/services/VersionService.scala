package com.socrata.cetera.resources

import scala.io.Source.fromInputStream

import com.rojoma.json.v3.util.JsonUtil.{readJson, renderJson}
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, JsonKeyStrategy, Strategy}
import com.rojoma.simplearm.v2._
import com.socrata.http.server.HttpRequest
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource
import org.slf4j.LoggerFactory

@JsonKeyStrategy(Strategy.Underscore)
case class Version(
    service: String,
    version: String,
    scalaVersion: String)

object Version {
  implicit val versionCodec = AutomaticJsonCodecBuilder[Version]

  val getVersion = using(classOf[Version].getResourceAsStream("/com/socrata/cetera/version")) { file =>
    val reader = fromInputStream(file).bufferedReader
    readJson[Version](reader)
  }
}

object VersionService {
  lazy val logger = LoggerFactory.getLogger(classOf[Version])

  object service extends SimpleResource {
    override val get = { (req: HttpRequest) =>
      logger.info(req.servletRequest.getRemoteHost + " is requesting the version number")
      Version.getVersion match {
        case Left(error) =>
          logger.error("Something went wrong trying to parse the version json file: " + error)
          InternalServerError ~> Json(error)
        case Right(version) =>
          logger.info("Reporting the version as: " + renderJson(version, pretty = true))
          OK ~> Json(version)
      }
    }
  }
}
