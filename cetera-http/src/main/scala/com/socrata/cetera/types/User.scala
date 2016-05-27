package com.socrata.cetera.types

import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, JsonKeyStrategy, JsonUtil, Strategy}
import org.slf4j.LoggerFactory

import com.socrata.cetera.util.JsonDecodeException

@JsonKeyStrategy(Strategy.Underscore)
case class User(id: String,
                screenName: Option[String],
                email: Option[String],
                roleName: Option[String],
                profileImageUrlLarge: Option[String],
                profileImageUrlMedium: Option[String],
                profileImageUrlSmall: Option[String])

object User {
  implicit val codec = AutomaticJsonCodecBuilder[User]
  val logger = LoggerFactory.getLogger(getClass)

  def apply(source: String): Option[User] = {
    Option(source).flatMap { s =>
      JsonUtil.parseJson[User](s) match {
        case Right(user) => Some(user)
        case Left(err) =>
          logger.error(err.english)
          throw new JsonDecodeException(err)
      }
    }
  }
}
