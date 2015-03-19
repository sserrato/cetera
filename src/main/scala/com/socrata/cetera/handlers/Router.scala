package com.socrata.cetera.handlers

import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleRouteContext._
import com.socrata.http.server.{HttpRequest, HttpResponse, HttpService}

import com.socrata.cetera.util.JsonResponses._

class Router(
    versionResource: => HttpService,
    catalogResource: => HttpService,
    domainsResource: => HttpService,
    categoriesResource: => HttpService,
    tagsResource: => HttpService) {

  val routes = Routes(
    Route("/version", versionResource),
    Route("/catalog", catalogResource),
    Route("/catalog/domains", domainsResource),
    Route("/catalog/categories", categoriesResource),
    Route("/catalog/tags", tagsResource))

  def route(req: HttpRequest): HttpResponse =
    routes(req.requestPath) match {
      case Some(s) =>
        s(req)
      case None =>
        NotFound ~> jsonError("not found")
    }
}
