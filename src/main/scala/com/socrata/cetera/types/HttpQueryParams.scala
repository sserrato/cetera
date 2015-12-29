package com.socrata.cetera.types

import com.socrata.http.server.HttpRequest

object HttpQueryParams {
  type MultiQueryParams = Map[String,Seq[String]]

  implicit class HttpRequestCompanion(req: HttpRequest) {
    def multiQueryParams: MultiQueryParams =
      req.allQueryParameters.mapValues(_.flatten)
  }
}
