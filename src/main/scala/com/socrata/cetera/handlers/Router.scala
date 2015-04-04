package com.socrata.cetera.handlers

import com.socrata.http.server.implicits._
import com.socrata.http.server.responses._
import com.socrata.http.server.routing.SimpleRouteContext._
import com.socrata.http.server.{HttpRequest, HttpResponse, HttpService}

import com.socrata.cetera.types._
import com.socrata.cetera.util.JsonResponses._

// Now the router knows about our ES field names
class Router(
    versionResource: => HttpService,
    catalogResource: => HttpService,
    countResource: CeteraFieldType with Groupable => HttpService) {

  val routes = Routes(
    // /version is for internal use
    Route("/version", versionResource),

    // general document search
    Route("/catalog", catalogResource),
    Route("/catalog/v1", catalogResource),

    // document counts for queries grouped by domain
    Route("/catalog/domains", countResource(DomainFieldType)),
    Route("/catalog/v1/domains", countResource(DomainFieldType)),

    // document counts for queries grouped by category
    Route("/catalog/categories", countResource(CategoriesFieldType)),
    Route("/catalog/v1/categories", countResource(CategoriesFieldType)),

    // document counts for queries grouped by tag
    Route("/catalog/tags", countResource(TagsFieldType)),
    Route("/catalog/v1/tags", countResource(TagsFieldType))
  )

  def route(req: HttpRequest): HttpResponse =
    routes(req.requestPath) match {
      case Some(s) =>
        s(req)
      case None =>
        NotFound ~> Header("Access-Control-Allow-Origin", "*") ~> jsonError("not found")
    }
}
