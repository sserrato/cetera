package com.socrata.cetera.util

import javax.servlet.http.HttpServletResponse

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
