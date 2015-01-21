package com.socrata.cetera.util

import com.socrata.http.server.responses._

object JsonResponses {
  def jsonMessage(message: String) = {
    val messageMap = Map("message" -> message)
    Json(messageMap)
  }

  def jsonError(error: String) = {
    val errorMap = Map("error" -> error)
    Json(errorMap)
  }
}
