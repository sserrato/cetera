package com.socrata.cetera.services

import scala.io.Source.fromInputStream

import com.rojoma.json.v3.io.JsonReader.fromReader
import com.rojoma.json.v3.util.JsonUtil.{readJson, renderJson}
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, JsonKeyStrategy, Strategy}
import com.rojoma.simplearm.v2._
import com.socrata.http.server.HttpRequest
import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleResource
import org.slf4j.LoggerFactory

object Stub {
  val getStub = using(getClass.getResourceAsStream("/stubbed_response.json")) { file =>
    val reader = fromInputStream(file).bufferedReader
    fromReader(reader)
  }
}

object StubService {
  lazy val logger = LoggerFactory.getLogger(getClass)

  object Service extends SimpleResource {
    override val get = { (req: HttpRequest) =>
      logger.info(req.servletRequest.getRemoteHost + " is calling for the stub")
      OK ~> Json(Stub.getStub)
    }
  }
}
