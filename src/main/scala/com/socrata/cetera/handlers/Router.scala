package com.socrata.cetera.handlers

import com.socrata.http.server.implicits._
import com.socrata.http.server.responses.NotFound
import com.socrata.http.server.routing.SimpleRouteContext.{Route, Routes}
import com.socrata.http.server.{HttpRequest, HttpResponse, HttpService}

import com.socrata.cetera.HeaderAclAllowOriginAll
import com.socrata.cetera.types._
import com.socrata.cetera.util.JsonResponses.jsonError

// $COVERAGE-OFF$ jetty wiring
// Now the router knows about our ES field names
class Router(
    versionResource: => HttpService,
    catalogResource: => HttpService,
    facetResource: String => HttpService,
    countResource: CeteraFieldType with Countable with Rawable => HttpService) {

  val routes = Routes(
    // /version is for internal use
    Route("/version", versionResource),

    // general document search
    Route("/catalog", catalogResource),
    Route("/catalog/v1", catalogResource),

    // document counts for queries grouped by domain
    Route("/catalog/domains", countResource(DomainFieldType)),
    Route("/catalog/v1/domains", countResource(DomainFieldType)),

    // facets by domain
    Route("/catalog/domains/{String}/facets", facetResource),
    Route("/catalog/v1/domains/{String}/facets", facetResource),

    // document counts for queries grouped by category
    Route("/catalog/categories", countResource(CategoriesFieldType)),
    Route("/catalog/v1/categories", countResource(CategoriesFieldType)),

    // document counts for queries grouped by tag
    Route("/catalog/tags", countResource(TagsFieldType)),
    Route("/catalog/v1/tags", countResource(TagsFieldType)),

    // document counts for queries grouped by domain_category
    Route("/catalog/domain_categories", countResource(DomainCategoryFieldType)),
    Route("/catalog/v1/domain_categories", countResource(DomainCategoryFieldType))
  )

  def route(req: HttpRequest): HttpResponse =
    routes(req.requestPath) match {
      case Some(s) => s(req)
      case None => NotFound ~> HeaderAclAllowOriginAll ~> jsonError("not found")
    }
}
// $COVERAGE-ON$
