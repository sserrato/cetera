package com.socrata.cetera.services

import java.io.File

import com.rojoma.json.v3.ast.{JString, JValue}
import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.CompactJsonWriter
import com.socrata.http.server.responses._
import org.mockserver.integration.ClientAndServer._
import org.mockserver.model.HttpRequest._
import org.mockserver.model.HttpResponse._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike, Matchers}

import com.socrata.cetera.auth.AuthParams
import com.socrata.cetera.errors.UnauthorizedError
import com.socrata.cetera.handlers.Params
import com.socrata.cetera.handlers.util.MultiQueryParams
import com.socrata.cetera.metrics.BalboaClient
import com.socrata.cetera.response.{SearchResult, SearchResults}
import com.socrata.cetera.search.{DocumentClient, DomainClient}
import com.socrata.cetera.types.Document
import com.socrata.cetera.{response => _, _}

class SearchServiceSpecWithPrivateData
  extends FunSuiteLike
  with Matchers
  with TestESData
  with BeforeAndAfterAll
  with BeforeAndAfterEach {
  val client = new TestESClient(testSuiteName)

  val coreTestPort = 8038
  val mockServer = startClientAndServer(coreTestPort)
  val httpClient = new TestHttpClient()
  val coreClient = new TestCoreClient(httpClient, coreTestPort)

  val domainClient = new DomainClient(client, coreClient, testSuiteName)
  val documentClient = new DocumentClient(client, domainClient, testSuiteName, None, None, Set.empty)
  val balboaDir = new File("balboa_test_trash")
  val balboaClient = new BalboaClient(balboaDir.getName)

  val service = new SearchService(documentClient, domainClient, balboaClient, coreClient)
  val cookie = "C = Cookie"
  val allDomainsParams = Map(
    "domains" -> domains.map(_.domainCname).mkString(","),
    "show_visibility" -> "true"
  ).mapValues(Seq(_))

  def emptyAndRemoveDir(dir: File): Unit = {
    if (dir.isDirectory) {
      dir.listFiles().foreach(f => f.delete())
    }
    dir.delete()
  }

  override protected def beforeAll(): Unit = {
    bootstrapData()
    emptyAndRemoveDir(balboaDir)
  }

  override def beforeEach(): Unit = {
    mockServer.reset()
  }

  override protected def afterAll(): Unit = {
    removeBootstrapData()
    client.close()
    httpClient.close()
    emptyAndRemoveDir(balboaDir)
  }

  private def prepareAuthenticatedUser(cookie: String, host: String, userJson: JValue): Unit = {
    val expectedRequest = request()
      .withMethod("GET")
      .withPath("/users.json")
      .withHeader(HeaderXSocrataHostKey, host)

    val expectedResponse =
      response()
        .withStatusCode(200)
        .withHeader("Content-Type", "application/json; charset=utf-8")
        .withBody(CompactJsonWriter.toString(userJson))

    mockServer
      .when(expectedRequest)
      .respond(expectedResponse)
  }

  private def requestParams(
      host: Option[String],
      ownerUserId: Option[String],
      sharedToUserId: Option[String],
      showVisibility: Boolean = false)
    : MultiQueryParams =
    Seq(
      Some(Seq(Params.showVisibility -> showVisibility.toString)),
      host.map { cname =>
        Seq(
          Params.searchContext -> cname,
          Params.domains -> cname
        )
      },
      ownerUserId.map(u => Seq(Params.forUser -> u)),
      sharedToUserId.map(u => Seq(Params.sharedTo -> u))
    ).flatten.flatten.toMap.mapValues(Seq(_))

  private def validateRequest(
      cookie: Option[String], host: Option[String], domains: Option[String],
      owner: Option[String], sharedTo: Option[String],
      showVisibility: Boolean, requireAuth: Boolean,
      statusValidator: StatusResponse => Unit,
      resultsValidator: SearchResults[SearchResult] => Unit)
    : Unit = {
    val (responseStatus, responseResults, _, _) =
      service.doSearch(requestParams(domains, owner, sharedTo, showVisibility), requireAuth, AuthParams(cookie=cookie), host, None)
    statusValidator(responseStatus)
    resultsValidator(responseResults)
  }

  private def fxfs(searchResults: SearchResults[SearchResult]): Seq[String] =
    searchResults.results.map(_.resource.dyn.id.!.asInstanceOf[JString].string)

  private def fxfsVisibility(searchResults: SearchResults[SearchResult]): Map[String, Boolean] =
    searchResults.results.map { hit =>
      val fxf = hit.resource.dyn.id.!.asInstanceOf[JString].string
      val visibility = hit.metadata.visibleToAnonymous.getOrElse(fail())
      fxf -> visibility
    }.toMap

  private def authedUserBodyFromRole(role: String) = {
    j"""{
          "id" : "cook-mons",
          "roleName" : $role,
          "rights" : [ "eat_cookies", "spell_words_starting_with_c" ]
        }"""
  }

  test("searching when auth is required with a super admin shows everything from every domain") {
    val expectedFxfs = docs.map(_.socrataId.datasetId)

    val host = "annabelle.island.net"
    val authedUserBody = j"""{"id" : "who-am-i", "flags" : [ "admin" ]}"""
    prepareAuthenticatedUser(cookie, host, authedUserBody)
    val res = service.doSearch(allDomainsParams, requireAuth = true, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching when auth is required with an administrator/publisher/designer/viewer shows " +
    "a) everything from their domain, " +
    "b) anonymously visible views from unlocked domains " +
    "c) views they own/share") {
    val authenticatingDomain = domains(0)
    val withinDomain = docs.filter(d => d.socrataId.domainId == authenticatingDomain.domainId).map(d => d.socrataId.datasetId)
    val ownedByCookieMonster = docs.collect{ case d: Document if d.ownerId == "cook-mons" => d.socrataId.datasetId }
    ownedByCookieMonster should be(List("zeta-0006"))
    val sharedToCookieMonster = docs.collect{ case d: Document if d.sharedTo.contains("cook-mons") => d.socrataId.datasetId }
    sharedToCookieMonster should be(List("zeta-0004"))
    val expectedFxfs = (withinDomain ++ ownedByCookieMonster ++ sharedToCookieMonster ++ anonymouslyViewableDocIds).distinct

    val host = domains(0).domainCname
    val adminBody = authedUserBodyFromRole("administrator")
    prepareAuthenticatedUser(cookie, host, adminBody)
    val adminRes = service.doSearch(allDomainsParams, requireAuth = true, AuthParams(cookie = Some(cookie)), Some(host), None)
    fxfs(adminRes._2) should contain theSameElementsAs expectedFxfs

    val publisherBody = authedUserBodyFromRole("publisher")
    prepareAuthenticatedUser(cookie, host, publisherBody)
    val publisherRes = service.doSearch(allDomainsParams, requireAuth = true, AuthParams(cookie = Some(cookie)), Some(host), None)
    fxfs(publisherRes._2) should contain theSameElementsAs expectedFxfs

    val designerBody = authedUserBodyFromRole("designer")
    prepareAuthenticatedUser(cookie, host, designerBody)
    val designerRes = service.doSearch(allDomainsParams, requireAuth = true, AuthParams(cookie = Some(cookie)), Some(host), None)
    fxfs(designerRes._2) should contain theSameElementsAs expectedFxfs

    val viewerBody = authedUserBodyFromRole("viewer")
    prepareAuthenticatedUser(cookie, host, viewerBody)
    val viewerRes = service.doSearch(allDomainsParams, requireAuth = true, AuthParams(cookie = Some(cookie)), Some(host), None)
    fxfs(viewerRes._2) should contain theSameElementsAs expectedFxfs
  }

  test("searching when auth is required with an editor shows " +
    "a) anonymously visible views from their domain " +
    "b) anonymously visible views from unlocked domains " +
    "c) views they own/share") {
    val authenticatingDomain = domains(0)
    val ownedByCookieMonster = docs.collect{ case d: Document if d.ownerId == "cook-mons" => d.socrataId.datasetId }
    val sharedToCookieMonster = docs.collect{ case d: Document if d.sharedTo.contains("cook-mons") => d.socrataId.datasetId }
    val expectedFxfs = (ownedByCookieMonster ++ sharedToCookieMonster ++ anonymouslyViewableDocIds).distinct

    val host = domains(0).domainCname
    val editorBody = authedUserBodyFromRole("editor")
    prepareAuthenticatedUser(cookie, host, editorBody)
    val editorRes = service.doSearch(allDomainsParams, requireAuth = true, AuthParams(cookie = Some(cookie)), Some(host), None)
    fxfs(editorRes._2) should contain theSameElementsAs expectedFxfs
  }

  test("searching when auth is required with a roleless, but logged-in users shows " +
    "a) anonymously visible views " +
    "b) views they own/share") {
    val authenticatingDomain = domains(0)
    val ownedByCookieMonster = docs.collect{ case d: Document if d.ownerId == "cook-mons" => d.socrataId.datasetId }
    val sharedToCookieMonster = docs.collect{ case d: Document if d.sharedTo.contains("cook-mons") => d.socrataId.datasetId }
    val expectedFxfs = (ownedByCookieMonster ++ sharedToCookieMonster ++ anonymouslyViewableDocIds).distinct

    val host = domains(0).domainCname
    val editorBody = authedUserBodyFromRole("")
    prepareAuthenticatedUser(cookie, host, editorBody)
    val editorRes = service.doSearch(allDomainsParams, requireAuth = true, AuthParams(cookie = Some(cookie)), Some(host), None)
    fxfs(editorRes._2) should contain theSameElementsAs expectedFxfs
  }

  test("searching when auth is required without an extended host throws an UnauthorizedError") {
    val host = domains(0).domainCname
    val userBody = authedUserBodyFromRole("")
    prepareAuthenticatedUser(cookie, host, userBody)

    intercept[UnauthorizedError] {
      service.doSearch(allDomainsParams, requireAuth = true, AuthParams(cookie = Some(cookie)), None, None)
    }
  }

  test("searching when auth is required without any authParams throws an UnauthorizedError") {
    val host = domains(0).domainCname
    val userBody = authedUserBodyFromRole("")
    prepareAuthenticatedUser(cookie, host, userBody)

    intercept[UnauthorizedError] {
      service.doSearch(allDomainsParams, requireAuth = true, AuthParams(), Some(host), None)
    }
  }

  test("searching when auth is required without authenticating at all throws an UnauthorizedError") {
    val host = domains(0).domainCname
    val userBody = authedUserBodyFromRole("")
    prepareAuthenticatedUser(cookie, host, userBody)

    intercept[UnauthorizedError] {
      val params = requestParams(None, None, None, showVisibility = false)
      service.doSearch(params, requireAuth = true, AuthParams(), None, None)
    }
  }

  test("searching when auth is not required returns anonymously viewable views on unlocked domains") {
    val res = service.doSearch(allDomainsParams, requireAuth = false, AuthParams(), None, None)
    fxfs(res._2) should contain theSameElementsAs anonymouslyViewableDocIds
  }

  test("when requested, include post-calculated anonymous visibility field") {
    val host = "annabelle.island.net"
    val authedUserBody =
      j"""{
        "id" : "lil-john",
        "roleName" : "publisher",
        "rights" : [ "walk_though_forest", "laugh_back_and_forth", "reminisce" ]
        }"""
    val expectedFxfs = Map(
      "fxf-3" -> false,
      "fxf-7" -> false,
      "zeta-0002" -> true,
      "zeta-0005" -> true,
      "zeta-0009" -> false
    )

    prepareAuthenticatedUser(cookie, host, authedUserBody)
    validateRequest(
      Some(cookie), Some(host), Some(host), None, None, showVisibility = true, true,
      _ should be(OK),
      fxfsVisibility(_) should contain theSameElementsAs expectedFxfs
    )
  }

  test("federated search results also know their own visibility") {
    val host = "petercetera.net"
    val authedUserBody =
      j"""{
        "id" : "cook-mons",
        "roleName" : "viewer",
        "rights" : [ "eat_cookies", "spell_words_starting_with_c" ]
        }"""
    val expectedFxfs = Map(
      "fxf-0" -> true,
      "fxf-4" -> true,
      "fxf-8" -> true,
      "zeta-0001" -> true,
      "zeta-0004" -> false,
      "zeta-0006" -> false,
      "zeta-0007" -> true
    )

    prepareAuthenticatedUser(cookie, host, authedUserBody)
    validateRequest(
      Some(cookie), Some(host), Some(host), None, None, showVisibility = true, true,
      _ should be(OK),
      fxfsVisibility(_) should contain theSameElementsAs expectedFxfs
    )
  }

  test("searching for assets shared to logged in user returns nothing if nothing is shared") {
    val host = "petercetera.net"
    val authedUserBody =
      j"""{
        "id" : "No One",
        "roleName" : "nothing",
        "rights" : [ "nothing" ]
        }"""
    val expectedFxfs = None

    prepareAuthenticatedUser(cookie, host, authedUserBody)
    validateRequest(
      Some(cookie), Some(host), Some(host), None, Some("No One"), showVisibility = false, true,
      _ should be(OK),
      fxfs(_) should contain theSameElementsAs expectedFxfs
    )
  }

  test("searching for assets shared to logged in user works if a public/published asset is shared") {
    val host = "petercetera.net"
    val authedUserBody =
      j"""{
        "id" : "King Richard",
        "roleName" : "King",
        "rights" : [ "collect_taxes", "wear_crown" ]
        }"""
    val expectedFxfs = Seq("zeta-0007")

    prepareAuthenticatedUser(cookie, host, authedUserBody)
    validateRequest(
      Some(cookie), Some(host), Some(host), None, Some("King Richard"), showVisibility = false, true,
      _ should be(OK),
      fxfs(_) should contain theSameElementsAs expectedFxfs
    )
  }

  test("searching for assets shared to logged in user works if a private/unpublished asset is shared") {
    val host = "petercetera.net"
    val authedUserBody =
      j"""{
        "id" : "Little John",
        "roleName" : "secondInCommand",
        "rights" : [ "rob_from_the_rich", "give_to_the_poor" ]
        }"""
    val expectedFxfs = Seq("zeta-0006")

    prepareAuthenticatedUser(cookie, host, authedUserBody)
    validateRequest(
      Some(cookie), Some(host), Some(host), None, Some("Little John"), showVisibility = false, true,
      _ should be(OK),
      fxfs(_) should contain theSameElementsAs expectedFxfs
    )
  }

  test("searching for assets owned by logged in user returns nothing if user owns nothing") {
    val host = "petercetera.net"
    val authedUserBody =
      j"""{
        "id" : "No One",
        "roleName" : "nothing",
        "rights" : [ "nothing" ]
        }"""
    val expectedFxfs = None

    prepareAuthenticatedUser(cookie, host, authedUserBody)
    validateRequest(
      Some(cookie), Some(host), Some(host), Some("No One"), None, showVisibility = false, true,
      _ should be(OK),
      fxfs(_) should contain theSameElementsAs expectedFxfs
    )
  }

  test("searching for assets owned by logged in user returns all assets the user owns, " +
    "regardless of private/public/approval status and regardless of what domain they come from") {
    // this has robin-hood looking up his own data.
    val authenticatingDomain = domains(0).domainCname
    val params = Map("for_user" -> Seq("robin-hood"))
    val userBody = j"""{"id" : "robin-hood"}"""
    prepareAuthenticatedUser(cookie, authenticatingDomain, userBody)

    val docsOwnedByRobin = docs.filter(_.ownerId == "robin-hood")
    val ownedByRobinIds = docsOwnedByRobin.map(_.socrataId.datasetId).toSet
    val res = service.doSearch(params, requireAuth = true, AuthParams(cookie = Some(cookie)), Some(authenticatingDomain), None)

    // confirm that robin owns view on domains other than 0
    docsOwnedByRobin.filter(_.socrataId.domainId !=0) should not be('empty)
    fxfs(res._2) should contain theSameElementsAs ownedByRobinIds
  }

  test("searching for assets owned by another user (robin-hood) when the logged-in user (cook-mons) is an admin returns " +
    " a) any views that robin-hood owns on cook-mons' domain" +
    " b) anonymously viewable views on any unlocked domain") {
    // this has cook-mons (admin on domain 0) checking up on robin-hood
    val authenticatingDomain = domains(0).domainCname
    val params = Map("for_user" -> Seq("robin-hood"))
    prepareAuthenticatedUser(cookie, authenticatingDomain, authedUserBodyFromRole("administrator"))

    val ownedByRobin = docs.collect{ case d: Document if d.ownerId == "robin-hood" => d.socrataId.datasetId }.toSet
    val onDomain0 = docs.collect{ case d: Document if d.socrataId.domainId == 0 => d.socrataId.datasetId}.toSet
    val anonymouslyViewable = anonymouslyViewableDocIds.toSet
    val expectedFxfs = ownedByRobin & (onDomain0 ++ anonymouslyViewable)

    val res = service.doSearch(params, requireAuth = true, AuthParams(cookie = Some(cookie)), Some(authenticatingDomain), None)

    // confirm that robin has documents on other domains that are being excluded
    expectedFxfs.size < ownedByRobin.size should be(true)
    fxfs(res._2) should contain theSameElementsAs expectedFxfs
  }

  test("searching for assets shared to anyone except logged in user throws an unauthorizedError") {
    val host = "petercetera.net"
    val authedUserBody =
      j"""{
        "id" : "No One",
        "roleName" : "nothing",
        "rights" : [ "nothing" ]
        }"""

    prepareAuthenticatedUser(cookie, host, authedUserBody)
    intercept[UnauthorizedError] {
      val params = requestParams(Some(host), None, Some("Different Person"), showVisibility = false)
      service.doSearch(params, true, AuthParams(cookie=Some(cookie)), Some(host), None)
    }
  }

  test("searching for assets by sending for_user and shared_to params returns no results") {
    val host = "petercetera.net"
    val authedUserBody =
      j"""{
        "id" : "robin-hood",
        "roleName" : "leader",
        "rights" : [ "rob_from_the_rich", "give_to_the_poor", "get_all_the_glory" ]
        }"""

    prepareAuthenticatedUser(cookie, host, authedUserBody)
    validateRequest(
      Some(cookie), Some(host), Some(host), Some("robin-hood"), Some("robin-hood"), showVisibility = false, true,
      _ should be(OK),
      fxfs(_) should be('empty)
    )
  }

  // contrast this test to that in SearchServiceSpecWithTestData
  test("if a domain is locked and proper auth is provided, data should come back") {
    val lockedDomain = domains(8).domainCname
    val params = Map(
      "domains" -> lockedDomain,
      "search_context" -> lockedDomain
    ).mapValues(Seq(_))
    val userBody = authedUserBodyFromRole("publisher")
    prepareAuthenticatedUser(cookie, lockedDomain, userBody)
    val res = service.doSearch(params, false, AuthParams(Some(cookie)), Some(lockedDomain), None)._2.results
    val actualFxfs = res.map(_.resource.dyn.id.!.asInstanceOf[JString].string)
    val expectedFxfs = Seq("zeta-0008")
    actualFxfs should contain theSameElementsAs (expectedFxfs)
  }

  test("hidden documents should not be hidden to users who can see everything on their domain") {
    val host = domains(0).domainCname
    val hiddenDoc = docs(4)
    val userBody = authedUserBodyFromRole("publisher")
    prepareAuthenticatedUser(cookie, host, userBody)

    val (_, res, _, _) = service.doSearch(Map(
      Params.ids -> hiddenDoc.socrataId.datasetId
    ).mapValues(Seq(_)), true, AuthParams(cookie = Some(cookie)), Some(host), None)
    val actualFxfs = res.results.map(_.resource.dyn.id.!.asInstanceOf[JString].string)
    actualFxfs(0) should be(hiddenDoc.socrataId.datasetId)
  }

  test("searching as a superadmin with public=true, should find all public views") {
    val expectedFxfs = docs.filter(d => d.isPublic).map(_.socrataId.datasetId)

    val host = "annabelle.island.net"
    val authedUserBody = j"""{"id" : "who-am-i", "flags" : [ "admin" ]}"""
    prepareAuthenticatedUser(cookie, host, authedUserBody)
    val params = allDomainsParams ++ Map("public" -> Seq("true"))
    val res = service.doSearch(params, requireAuth = true, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching as a superadmin with public=false, should find all private views") {
    val expectedFxfs = docs.filter(d => !d.isPublic).map(_.socrataId.datasetId)

    val host = "annabelle.island.net"
    val authedUserBody = j"""{"id" : "who-am-i", "flags" : [ "admin" ]}"""
    prepareAuthenticatedUser(cookie, host, authedUserBody)
    val params = allDomainsParams ++ Map("public" -> Seq("false"))
    val res = service.doSearch(params, requireAuth = true, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching as a superadmin with published=true, should find all published views") {
    val expectedFxfs = docs.filter(d => d.isPublished).map(_.socrataId.datasetId)

    val host = "annabelle.island.net"
    val authedUserBody = j"""{"id" : "who-am-i", "flags" : [ "admin" ]}"""
    prepareAuthenticatedUser(cookie, host, authedUserBody)
    val params = allDomainsParams ++ Map("published" -> Seq("true"))
    val res = service.doSearch(params, requireAuth = true, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching as a superadmin with published=false, should find all unpublished views") {
    val expectedFxfs = docs.filter(d => !d.isPublished).map(_.socrataId.datasetId)

    val host = "annabelle.island.net"
    val authedUserBody = j"""{"id" : "who-am-i", "flags" : [ "admin" ]}"""
    prepareAuthenticatedUser(cookie, host, authedUserBody)
    val params = allDomainsParams ++ Map("published" -> Seq("false"))
    val res = service.doSearch(params, requireAuth = true, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching as a superadmin with derived=true, should find all derived views") {
    val expectedFxfs = docs.filter(d => !d.isDefaultView).map(_.socrataId.datasetId)

    val host = "annabelle.island.net"
    val authedUserBody = j"""{"id" : "who-am-i", "flags" : [ "admin" ]}"""
    prepareAuthenticatedUser(cookie, host, authedUserBody)
    val params = allDomainsParams ++ Map("derived" -> Seq("true"))
    val res = service.doSearch(params, requireAuth = true, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching as a superadmin with derived=false, should find all default views") {
    val expectedFxfs = docs.filter(d => d.isDefaultView).map(_.socrataId.datasetId)

    val host = "annabelle.island.net"
    val authedUserBody = j"""{"id" : "who-am-i", "flags" : [ "admin" ]}"""
    prepareAuthenticatedUser(cookie, host, authedUserBody)
    val params = allDomainsParams ++ Map("derived" -> Seq("false"))
    val res = service.doSearch(params, requireAuth = true, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching as a superadmin with explicity_hidden=true, should find all hidden views") {
    val expectedFxfs = docs.filter(d => d.hideFromCatalog.getOrElse(false)).map(_.socrataId.datasetId)

    val host = "annabelle.island.net"
    val authedUserBody = j"""{"id" : "who-am-i", "flags" : [ "admin" ]}"""
    prepareAuthenticatedUser(cookie, host, authedUserBody)
    val params = allDomainsParams ++ Map("explicitly_hidden" -> Seq("true"))
    val res = service.doSearch(params, requireAuth = true, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching as a superadmin with explicity_hidden=false, should find all not-hidden views") {
    val expectedFxfs = docs.filter(d => !d.hideFromCatalog.getOrElse(false)).map(_.socrataId.datasetId)

    val host = "annabelle.island.net"
    val authedUserBody = j"""{"id" : "who-am-i", "flags" : [ "admin" ]}"""
    prepareAuthenticatedUser(cookie, host, authedUserBody)
    val params = allDomainsParams ++ Map("explicitly_hidden" -> Seq("false"))
    val res = service.doSearch(params, requireAuth = true, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

}
