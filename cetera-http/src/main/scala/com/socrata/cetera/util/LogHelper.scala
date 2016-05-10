package com.socrata.cetera.util

import scala.io.Source

import com.socrata.http.client.RequestBuilder
import com.socrata.http.server.HttpRequest
import org.elasticsearch.action.search.SearchRequestBuilder

import com.socrata.cetera._

object LogHelper {
  // WARN: changing this will likely break Sumo (regex-based log parser)
  def formatRequest(request: HttpRequest, timings: InternalTimings): String = {
    List[String](
      "[" + request.servletRequest.getMethod + "]",
      request.requestPathStr,
      request.queryStr.getOrElse(""),
      "requested by",
      request.servletRequest.getRemoteHost,
      s"extended host = ${request.header(HeaderXSocrataHostKey)}",
      s"request id = ${request.header(HeaderXSocrataRequestIdKey)}",
      s"""TIMINGS ## ESTime : ${timings.searchMillis} ## ServiceTime : ${timings.serviceMillis}"""
    ).mkString(" -- ")
  }

  def formatEsRequest(search: SearchRequestBuilder): String = {
    s"""Elasticsearch request body: ${search.toString.replaceAll("""[\n\s]+""", " ")}
     """.stripMargin.trim
  }

  def formatHttpRequestVerbose(req: HttpRequest): String = {
    val sb = StringBuilder.newBuilder

    sb.append(s"received http request:\n")
    sb.append(s"${req.method} ${req.requestPath.mkString("/","/","")}${req.queryStr.map("?" + _).getOrElse("")}\n")
    req.headerNames.foreach { n =>
      sb.append(s"$n: ${req.headers(n).mkString(", ")}\n")
    }
    sb.append("\n")
    Source.fromInputStream(req.inputStream).getLines().foreach(line => sb.append(s"$line\n"))

    sb.toString()
  }

  def formatSimpleHttpRequestBuilderVerbose(req: RequestBuilder): String = {
    val sb = StringBuilder.newBuilder

    sb.append(s"sending http request:\n")
    sb.append(s"${req.method.getOrElse("GET")} ${req.path.mkString("/", "/", "")}\n")
    sb.append(req.headers.map { case (k,v) => s"$k: $v" }.mkString("\n"))

    sb.toString()
  }

  def formatHttpResponseHeaders(headers: Seq[String]): String = {
    val sb = StringBuilder.newBuilder

    sb.append(s"sending http response with headers:\n")
    sb.append(s"${headers.mkString("\n")}\n")

    sb.toString()
  }
}
