package com.socrata.cetera.util

import javax.servlet.http.HttpServletResponse

import com.rojoma.json.v3.codec.{JsonDecode,JsonEncode}
import com.rojoma.json.v3.util.AutomaticJsonCodecBuilder
import com.socrata.http.server.responses._

object JsonResponses {
  def jsonMessage(message: String): HttpServletResponse => Unit = {
    val messageMap = Map("message" -> message)
    Json(messageMap)
  }

  def jsonError(error: String): HttpServletResponse => Unit = {
    val errorMap = Map("error" -> error)
    Json(errorMap)
  }
}

// Helpers for internal timing information to report to the caller
case class InternalTimings(serviceElapsedMillis: Long, searchMillis: Option[Long])

object InternalTimings {
  implicit val jCodec = AutomaticJsonCodecBuilder[InternalTimings]
}

case class SearchResults[T: JsonEncode](results: Seq[T], timings: Option[InternalTimings] = None)

object SearchResults {
  implicit def jCodec[T: JsonEncode: JsonDecode] = AutomaticJsonCodecBuilder[SearchResults[T]]
}
