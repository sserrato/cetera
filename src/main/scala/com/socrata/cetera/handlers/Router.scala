package com.socrata.cetera.handlers

import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleRouteContext._
import com.socrata.http.server.{HttpRequest, HttpResponse, HttpService}

import com.socrata.cetera.util.JsonResponses._

// Now the router knows about our ES field names
class Router(
    versionResource: => HttpService,
    catalogResource: => HttpService,
    countResource: String => HttpService) {

  val routes = Routes(
    Route("/version", versionResource),
    Route("/catalog", catalogResource),
    Route("/catalog/domains", countResource("socrata_id.domain_cname.raw")),
    Route("/catalog/categories", countResource("animl_annotations.category_names.raw")),
    Route("/catalog/tags", countResource("animl_annotations.tag_names.raw")))

  def route(req: HttpRequest): HttpResponse =
    routes(req.requestPath) match {
      case Some(s) =>
        s(req)
      case None =>
        NotFound ~> jsonError("not found")
    }
}
