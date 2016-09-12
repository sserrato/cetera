package com.socrata.cetera.response

import java.io.{PrintWriter, StringWriter}

import com.rojoma.json.v3.ast.{JString, JValue}
import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.util._
import com.socrata.http.server.HttpResponse
import com.socrata.http.server.responses.{Json, StatusResponse, Unauthorized}

object JsonResponses {
  def jsonError(error: String): HttpResponse = {
    val errorMap = Map("error" -> error)
    Json(errorMap)
  }

  def jsonError(context: String, error: Throwable): HttpResponse = {
    Json(Map("context" -> context,
      "error" -> error.toString,
      "stackTrace" -> getStackTraceAsString(error)
    ))}

  private

  // from http://alvinalexander.com/scala/how-convert-stack-trace-exception-string-print-logger-logging-log4j-slf4j
  def getStackTraceAsString(t: Throwable) = {
    val sw = new StringWriter
    t.printStackTrace(new PrintWriter(sw))
    sw.toString
  }
}

// Helpers for internal timing information to report to the caller
case class InternalTimings(serviceMillis: Long, searchMillis: Seq[Long])

object InternalTimings {
  implicit val jCodec = AutomaticJsonCodecBuilder[InternalTimings]
}

@JsonKeyStrategy(Strategy.Underscore)
case class Classification(
  categories: Seq[JValue],
  tags: Seq[JValue],
  domainCategory: Option[JValue],
  domainTags: Option[JValue],
  domainMetadata: Option[JValue])

object Classification {
  implicit val jCodec = AutomaticJsonCodecBuilder[Classification]
}

@JsonKeyStrategy(Strategy.Underscore)
case class Metadata(
    domain: String,
    isPublic: Option[Boolean] = None,
    isPublished: Option[Boolean] = None,
    isModerationApproved: Option[Boolean] = None,
    isRoutingApproved: Option[Boolean] = None,
    isDatalensApproved: Option[Boolean] = None,
    visibleToAnonymous: Option[Boolean] = None,
    score: Option[BigDecimal] = None,
    grants: Option[Seq[JValue]] = None)

object Metadata {
  implicit val jCodec = AutomaticJsonCodecBuilder[Metadata]
}

@JsonKeyStrategy(Strategy.Underscore)
case class SearchResult(
  resource: JValue,
  classification: Classification,
  metadata: Metadata,
  permalink: JString,
  link: JString,
  previewImageUrl: Option[JString])

object SearchResult {
  implicit val jCodec = AutomaticJsonCodecBuilder[SearchResult]
}

case class SearchResults[T](results: Seq[T],
                            resultSetSize: Long,
                            timings: Option[InternalTimings] = None)

object SearchResults {
  implicit def jEncode[T : JsonEncode]: JsonEncode[SearchResults[T]] = {
    AutomaticJsonEncodeBuilder[SearchResults[T]]
  }

  def returnUnauthorized[T](setCookies: Seq[String], time: Long)
  : (StatusResponse, SearchResults[T], InternalTimings, Seq[String]) =
    (
      Unauthorized,
      SearchResults(Seq.empty[T], 0),
      InternalTimings(Timings.elapsedInMillis(time), Seq(0)),
      setCookies
      )
}
