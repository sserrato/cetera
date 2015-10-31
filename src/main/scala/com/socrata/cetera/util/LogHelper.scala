package com.socrata.cetera.util

import com.socrata.http.server.HttpRequest

object LogHelper {
  // WARN: changing this will likely break Sumo (regex-based log parser)
  def formatRequest(request: HttpRequest, timings: InternalTimings): String = {
    List[String](
      "[" + request.servletRequest.getMethod + "]",
      request.requestPathStr,
      request.queryStr.getOrElse(""),
      "requested by",
      request.servletRequest.getRemoteHost,
      s"""TIMINGS ## ESTime : ${timings.searchMillis.getOrElse(-1)} ## ServiceTime : ${timings.serviceMillis}"""
    ).mkString(" -- ")
  }
}
