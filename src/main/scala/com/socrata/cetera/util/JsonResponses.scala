package com.socrata.cetera.util

import javax.servlet.http.HttpServletResponse

import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, AutomaticJsonEncodeBuilder}
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

  def jsonError(context: String, error: Throwable): HttpServletResponse => Unit = {
    Json(Map("context" -> context,
      "error" -> error.toString,
      "stackTrace" -> error.getStackTraceString
    ))}
}

// Helpers for internal timing information to report to the caller
case class InternalTimings(serviceMillis: Long, searchMillis: Option[Long])

object InternalTimings {
  implicit val jCodec = AutomaticJsonCodecBuilder[InternalTimings]
}

case class SearchResults[T](results: Seq[T],
                            resultSetSize: Option[Long] = None,
                            timings: Option[InternalTimings] = None)

object SearchResults {
  implicit def jEncode[T : JsonEncode]: JsonEncode[SearchResults[T]] = {
    AutomaticJsonEncodeBuilder[SearchResults[T]]
  }
}
