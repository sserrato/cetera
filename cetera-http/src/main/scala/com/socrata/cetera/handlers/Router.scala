package com.socrata.cetera.handlers

import com.socrata.http.server.implicits._
import com.socrata.http.server.responses.NotFound
import com.socrata.http.server.routing.SimpleRouteContext.{Route, Routes}
import com.socrata.http.server.{HttpRequest, HttpResponse, HttpService}

import com.socrata.cetera._
import com.socrata.cetera.response.JsonResponses.jsonError
import com.socrata.cetera.search.Visibility
import com.socrata.cetera.types._

// $COVERAGE-OFF$ jetty wiring
// Now the router knows about our ES field names
class Router(
    versionResource: => HttpService,
    catalogResource: Visibility => HttpService,
    facetResource: String => HttpService,
    domainCountResource: => HttpService,
    countResource: DocumentFieldType with Countable with Rawable => HttpService,
    userSearchResource: => HttpService) {

  val routes = Routes(
    // /version is for internal use
    Route("/version", versionResource),

    // general document search
    Route("/catalog", catalogResource(Visibility.anonymous)),
    Route("/catalog/v1", catalogResource(Visibility.anonymous)),

    // document counts for queries grouped by domain
    Route("/catalog/domains", domainCountResource),
    Route("/catalog/v1/domains", domainCountResource),

    // facets by domain
    Route("/catalog/domains/{String}/facets", facetResource),
    Route("/catalog/v1/domains/{String}/facets", facetResource),

    // document counts for queries grouped by category
    Route("/catalog/categories", countResource(CategoriesFieldType)),
    Route("/catalog/v1/categories", countResource(CategoriesFieldType)),

    // document counts for queries grouped by tag
    Route("/catalog/tags", countResource(TagsFieldType)),
    Route("/catalog/v1/tags", countResource(TagsFieldType)),

    // document counts for queries grouped by owner
    Route("/catalog/owners", countResource(OwnerIdFieldType)),
    Route("/catalog/v1/owners", countResource(OwnerIdFieldType)),

    // document counts for queries grouped by domain_category
    Route("/catalog/domain_categories", countResource(DomainCategoryFieldType)),
    Route("/catalog/v1/domain_categories", countResource(DomainCategoryFieldType)),

    // document counts for queries grouped by domain_tags
    Route("/catalog/domain_tags", countResource(DomainTagsFieldType)),
    Route("/catalog/v1/domain_tags", countResource(DomainTagsFieldType)),

    //
    // Internal endpoints
    //

    // internal asset selector
    Route("/asset_selector", catalogResource(Visibility.assetSelector)),
    Route("/asset_selector/v1", catalogResource(Visibility.assetSelector)),

    // internal search for assets owned by a user
    Route("/personal_catalog", catalogResource(Visibility.personalCatalog)),
    Route("/personal_catalog/v1", catalogResource(Visibility.personalCatalog)),

    // internal user search
    Route("/whitepages", userSearchResource),
    Route("/whitepages/v1", userSearchResource)
  )

  def route(req: HttpRequest): HttpResponse =
    routes(req.requestPath) match {
      case Some(s) => s(req)
      case None => NotFound ~> HeaderAclAllowOriginAll ~> jsonError("not found")
    }
}
// $COVERAGE-ON$
