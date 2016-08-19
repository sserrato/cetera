package com.socrata.cetera.types

import com.rojoma.json.v3.util.{JsonUtil, AutomaticJsonCodecBuilder, Strategy, JsonKeyStrategy}
import org.slf4j.LoggerFactory

import com.socrata.cetera.util.JsonDecodeException

@JsonKeyStrategy(Strategy.Underscore)
case class Domain(domainId: Int,
                  domainCname: String,
                  siteTitle: Option[String],
                  organization: Option[String],
                  isCustomerDomain: Boolean,
                  moderationEnabled: Boolean,
                  routingApprovalEnabled: Boolean,
                  lockedDown: Boolean,
                  apiLockedDown: Boolean,
                  unmigratedNbeEnabled: Boolean) {

  def isLocked: Boolean = lockedDown || apiLockedDown
}

object Domain {
  implicit val jCodec = AutomaticJsonCodecBuilder[Domain]
  val logger = LoggerFactory.getLogger(getClass)

  def apply(source: String): Option[Domain] = {
    Option(source).flatMap { s =>
      JsonUtil.parseJson[Domain](s) match {
        case Right(domain) => Some(domain)
        case Left(err) =>
          logger.error(err.english)
          throw new JsonDecodeException(err)
      }
    }
  }
}
