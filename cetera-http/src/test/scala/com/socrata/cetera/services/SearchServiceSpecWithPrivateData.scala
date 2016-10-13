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
import com.socrata.cetera.types.{ApprovalStatus, Document}
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

  private def getAllPossibleResults(): Seq[SearchResult] = {
    val host = domains(0).domainCname
    val authedUserBody = j"""{"id" : "who-am-i", "flags" : [ "admin" ]}"""
    prepareAuthenticatedUser(cookie, host, authedUserBody)
    service.doSearch(allDomainsParams, requireAuth = true, AuthParams(cookie=Some(cookie)), Some(host), None)._2.results
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

  private def fxfs(searchResult: SearchResult): String =
    searchResult.resource.dyn.id.!.asInstanceOf[JString].string

  private def fxfs(searchResults: SearchResults[SearchResult]): Seq[String] =
    searchResults.results.map(fxfs)

  private def fxfsVisibility(searchResults: SearchResults[SearchResult]): Map[String, Boolean] =
    searchResults.results.map { hit =>
      val fxf = hit.resource.dyn.id.!.asInstanceOf[JString].string
      val visibility = hit.metadata.visibleToAnonymous.getOrElse(fail())
      fxf -> visibility
    }.toMap

  private def isApproved(res: SearchResult): Boolean = {
    val raApproved = res.metadata.isRoutingApproved.map(identity(_)).getOrElse(true) // None implies not relevant (so approved by default)
    val vmApproved = res.metadata.isModerationApproved.map(identity(_)).getOrElse(true)
    val dlApproved = res.metadata.isDatalensApproved.map(identity(_)).getOrElse(true)
    raApproved && vmApproved && dlApproved
  }

  val superAdminBody = j"""{"id" : "who-am-i", "flags" : [ "admin" ]}"""

  private def authedUserBodyFromRole(role: String) = {
    j"""{
          "id" : "cook-mons",
          "roleName" : $role,
          "rights" : [ "eat_cookies", "spell_words_starting_with_c" ]
        }"""
  }

  test("a basic search without auth on a basic domain (no VM, no RA) should return the expected results") {
    val basicDomain = domains(0).domainCname
    val domain0Docs = docs.filter(d => d.socrataId.domainId == 0)
    val allPossibleResults = getAllPossibleResults()
    val approvedFxfs = allPossibleResults.filter(isApproved).map(fxfs)
    val expectedFxfs = domain0Docs.filter(d =>
      d.isPublic && d.isPublished && !d.hideFromCatalog.getOrElse(false) && approvedFxfs.contains(d.socrataId.datasetId)).map(_.socrataId.datasetId)

    val params = Map("search_context" -> basicDomain, "domains" -> basicDomain).mapValues(Seq(_))
    val actualFxfs = fxfs(service.doSearch(params, requireAuth = false, AuthParams(), None, None)._2)
    expectedFxfs shouldNot be('empty)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("a basic search without auth on a moderated domain (but no RA) should return the expected results") {
    val moderatedDomain = domains(1).domainCname
    val domain1Docs = docs.filter(d => d.socrataId.domainId == 1)
    val allPossibleResults = getAllPossibleResults()
    val approvedFxfs = allPossibleResults.filter(isApproved).map(fxfs)
    val expectedFxfs = domain1Docs.filter(d =>
      d.isPublic && d.isPublished && !d.hideFromCatalog.getOrElse(false) && approvedFxfs.contains(d.socrataId.datasetId)).map(_.socrataId.datasetId)

    val params = Map("search_context" -> moderatedDomain, "domains" -> moderatedDomain).mapValues(Seq(_))
    val actualFxfs = fxfs(service.doSearch(params, requireAuth = false, AuthParams(), None, None)._2)
    expectedFxfs shouldNot be('empty)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("a basic search without auth on an RA-enabled domain (but no VM) should return the expected results") {
    val raEnabledDomain = domains(2).domainCname
    val domain2Docs = docs.filter(d => d.socrataId.domainId == 2)
    val allPossibleResults = getAllPossibleResults()
    val approvedFxfs = allPossibleResults.filter(isApproved).map(fxfs)
    val expectedFxfs = domain2Docs.filter(d =>
      d.isPublic && d.isPublished && !d.hideFromCatalog.getOrElse(false) && approvedFxfs.contains(d.socrataId.datasetId)).map(_.socrataId.datasetId)

    val params = Map("search_context" -> raEnabledDomain, "domains" -> raEnabledDomain).mapValues(Seq(_))
    val actualFxfs = fxfs(service.doSearch(params, requireAuth = false, AuthParams(), None, None)._2)
    expectedFxfs shouldNot be('empty)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("a basic search without auth on an RA & VM-enabled domain should return the expected results") {
    val raAndVmEnabledDomain = domains(3).domainCname
    val domain3Docs = docs.filter(d => d.socrataId.domainId == 3)
    val allPossibleResults = getAllPossibleResults()
    val approvedFxfs = allPossibleResults.filter(isApproved).map(fxfs)
    val expectedFxfs = domain3Docs.filter(d =>
      d.isPublic && d.isPublished && !d.hideFromCatalog.getOrElse(false) && approvedFxfs.contains(d.socrataId.datasetId)).map(_.socrataId.datasetId)

    val params = Map("search_context" -> raAndVmEnabledDomain, "domains" -> raAndVmEnabledDomain).mapValues(Seq(_))
    val actualFxfs = fxfs(service.doSearch(params, requireAuth = false, AuthParams(), None, None)._2)
    expectedFxfs shouldNot be('empty)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching when auth is required with a super admin shows everything from every domain") {
    val expectedFxfs = docs.map(_.socrataId.datasetId)

    val host = "annabelle.island.net"
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val res = service.doSearch(allDomainsParams, requireAuth = true, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains when auth is required with an administrator/publisher/designer/viewer shows " +
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

  test("searching across all domains when auth is required with an editor shows " +
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

  test("searching across all domains when auth is required with a roleless, but logged-in users shows " +
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

  test("searching across all domains when auth is not required returns anonymously viewable views on unlocked domains") {
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
      //"fxf-3" -> false,  // will be missing until we get all the RA info. this is not approved by domain 3, but should be in its queue
      //"fxf-7" -> false,  // will be missing until we get all the RA info. this is not approved by domain 3, but should be in its queue
      "zeta-0002" -> true,
      "zeta-0005" -> true,
      "zeta-0009" -> false,
      "zeta-0010" -> false
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
      "zeta-0007" -> true,
      "zeta-0011" -> false,
      "zeta-0012" -> true
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

  test("searching across all domains as a superadmin with public=true, should find all public views") {
    val expectedFxfs = docs.filter(d => d.isPublic).map(_.socrataId.datasetId)

    val host = "annabelle.island.net"
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("public" -> Seq("true"))
    val res = service.doSearch(params, requireAuth = true, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains as a superadmin with public=false, should find all private views") {
    val expectedFxfs = docs.filter(d => !d.isPublic).map(_.socrataId.datasetId)

    val host = "annabelle.island.net"
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("public" -> Seq("false"))
    val res = service.doSearch(params, requireAuth = true, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains as a superadmin with published=true, should find all published views") {
    val expectedFxfs = docs.filter(d => d.isPublished).map(_.socrataId.datasetId)

    val host = "annabelle.island.net"
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("published" -> Seq("true"))
    val res = service.doSearch(params, requireAuth = true, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains as a superadmin with published=false, should find all unpublished views") {
    val expectedFxfs = docs.filter(d => !d.isPublished).map(_.socrataId.datasetId)

    val host = "annabelle.island.net"
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("published" -> Seq("false"))
    val res = service.doSearch(params, requireAuth = true, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains as a superadmin with derived=true, should find all derived views") {
    val expectedFxfs = docs.filter(d => !d.isDefaultView).map(_.socrataId.datasetId)

    val host = "annabelle.island.net"
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("derived" -> Seq("true"))
    val res = service.doSearch(params, requireAuth = true, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains as a superadmin with derived=false, should find all default views") {
    val expectedFxfs = docs.filter(d => d.isDefaultView).map(_.socrataId.datasetId)

    val host = "annabelle.island.net"
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("derived" -> Seq("false"))
    val res = service.doSearch(params, requireAuth = true, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains as a superadmin with explicity_hidden=true, should find all hidden views") {
    val expectedFxfs = docs.filter(d => d.hideFromCatalog.getOrElse(false)).map(_.socrataId.datasetId)

    val host = "annabelle.island.net"
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("explicitly_hidden" -> Seq("true"))
    val res = service.doSearch(params, requireAuth = true, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains as a superadmin with explicity_hidden=false, should find all not-hidden views") {
    val expectedFxfs = docs.filter(d => !d.hideFromCatalog.getOrElse(false)).map(_.socrataId.datasetId)

    val host = "annabelle.island.net"
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("explicitly_hidden" -> Seq("false"))
    val res = service.doSearch(params, requireAuth = true, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains as a superadmin with approval_status=approved and no context should find all approved results") {
    val allPossibleResults = getAllPossibleResults()
    val expectedFxfs = allPossibleResults.filter(isApproved).map(fxfs)

    val host = "annabelle.island.net"
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("approval_status" -> Seq("approved"))
    val res = service.doSearch(params, requireAuth = true, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains as a superadmin with approval_status=approved and an unmoderated context should find all approved results") {
    val allPossibleResults = getAllPossibleResults()
    val expectedFxfs = allPossibleResults.filter(isApproved).map(fxfs)

    val host = domains(0).domainCname
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("approval_status" -> Seq("approved"), "search_context" -> Seq(host))
    val res = service.doSearch(params, requireAuth = true, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains as a superadmin with approval_status=approved and a moderated context should find only approved default views from unmoderated domains and approved views from moderated domains") {
    // this scenario is a little funny. the moderated search context says 'throw out all derived views from unmoderated domains'
    val allPossibleResults = getAllPossibleResults()
    val approvedFxfs = allPossibleResults.filter(isApproved).map(fxfs)
    val unmoderatedDomainIds = domains.filter(!_.moderationEnabled).map(_.domainId)
    val derivedViewsFromUnmoderatedDomains = docs.filter(d => !d.isDefaultView && unmoderatedDomainIds.contains(d.socrataId.domainId)).map(_.socrataId.datasetId)
    val expectedFxfs = approvedFxfs.toSet -- derivedViewsFromUnmoderatedDomains.toSet

    val host = domains(1).domainCname
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("approval_status" -> Seq("approved"), "search_context" -> Seq(host))
    val res = service.doSearch(params, requireAuth = true, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains as a superadmin with approval_status=pending and no context should find all pending results") {
    val moderatedDomainIds = domains.filter(_.moderationEnabled).map(_.domainId)
    val canbeModeratedDocs = docs.filter(d => d.datatype.startsWith("datalens") || moderatedDomainIds.contains(d.socrataId.domainId))
    val expectedFxfs = canbeModeratedDocs.filter(_.moderationStatus.getOrElse("") == "pending").map(_.socrataId.datasetId)

    val host = "annabelle.island.net"
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("approval_status" -> Seq("pending"))
    val res = service.doSearch(params, requireAuth = true, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains as a superadmin with approval_status=pending and an unmoderated context should find all pending results") {
    val moderatedDomainIds = domains.filter(_.moderationEnabled).map(_.domainId)
    val canbeModeratedDocs = docs.filter(d => d.datatype.startsWith("datalens") || moderatedDomainIds.contains(d.socrataId.domainId))
    val expectedFxfs = canbeModeratedDocs.filter(_.moderationStatus.getOrElse("") == "pending").map(_.socrataId.datasetId)

    val host = domains(0).domainCname
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("approval_status" -> Seq("pending"), "search_context" -> Seq(host))
    val res = service.doSearch(params, requireAuth = true, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains as a superadmin with approval_status=pending and a moderated context should find all pending results from moderated domains") {
    // Note that pending DL from unmoderated domains should not show here, b/c the moderated search context removes all
    // derived views from unmoderated domains
    val moderatedDomainIds = domains.filter(_.moderationEnabled).map(_.domainId)
    val fromModeratedDomainDocs = docs.filter(d => moderatedDomainIds.contains(d.socrataId.domainId))
    val expectedFxfs = fromModeratedDomainDocs.filter(_.moderationStatus.getOrElse("") == "pending").map(_.socrataId.datasetId)

    val host = domains(1).domainCname
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("approval_status" -> Seq("pending"), "search_context" -> Seq(host))
    val res = service.doSearch(params, requireAuth = true, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains as a superadmin with approval_status=rejected and no context should find all rejected results") {
    val moderatedDomainIds = domains.filter(_.moderationEnabled).map(_.domainId)
    val canbeModeratedDocs = docs.filter(d => d.datatype.startsWith("datalens") || moderatedDomainIds.contains(d.socrataId.domainId))
    val expectedFxfs = canbeModeratedDocs.filter(_.moderationStatus.getOrElse("") == "rejected").map(_.socrataId.datasetId)

    val host = "annabelle.island.net"
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("approval_status" -> Seq("rejected"))
    val res = service.doSearch(params, requireAuth = true, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains as a superadmin with approval_status=rejected and an unmoderated context should find all rejected results") {
    val moderatedDomainIds = domains.filter(_.moderationEnabled).map(_.domainId)
    val canbeModeratedDocs = docs.filter(d => d.datatype.startsWith("datalens") || moderatedDomainIds.contains(d.socrataId.domainId))
    val expectedFxfs = canbeModeratedDocs.filter(_.moderationStatus.getOrElse("") == "rejected").map(_.socrataId.datasetId)

    val host = domains(0).domainCname
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("approval_status" -> Seq("rejected"), "search_context" -> Seq(host))
    val res = service.doSearch(params, requireAuth = true, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching across all domains as a superadmin with approval_status=rejected and a moderated context should find all rejected results") {
    // Note that rejected DL from unmoderated domains should not show here, b/c the moderated search context removes all
    // derived views from unmoderated domains
    val moderatedDomainIds = domains.filter(_.moderationEnabled).map(_.domainId)
    val fromModeratedDomainDocs = docs.filter(d => moderatedDomainIds.contains(d.socrataId.domainId))
    val expectedFxfs = fromModeratedDomainDocs.filter(_.moderationStatus.getOrElse("") == "rejected").map(_.socrataId.datasetId)

    val host = domains(1).domainCname
    prepareAuthenticatedUser(cookie, host, superAdminBody)
    val params = allDomainsParams ++ Map("approval_status" -> Seq("rejected"), "search_context" -> Seq(host))
    val res = service.doSearch(params, requireAuth = true, AuthParams(cookie=Some(cookie)), Some(host), None)
    val actualFxfs = fxfs(res._2)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching as an admin on a moderated domain (as both search_context & domain), should find the correct set of views for the given approval_status") {
    val moderatedDomain = domains(1).domainCname
    val allPossibleResults = getAllPossibleResults()
    val approvedFxfs = allPossibleResults.filter(isApproved).map(fxfs)
    val domain1Docs = docs.filter(d => d.socrataId.domainId == 1)
    // approved views are views that pass all 3 types of approval
    val expectedApprovedFxfs = domain1Docs.filter(d => approvedFxfs.contains(d.socrataId.datasetId)).map(_.socrataId.datasetId)
    // rejected views are simply any views that are rejected
    val expectedRejectedFxfs = domain1Docs.filter(d => d.moderationStatus.getOrElse("") == "rejected").map(_.socrataId.datasetId)
    // pending views are simply any views that are pending
    val expectedPendingFxfs = domain1Docs.filter(d => d.moderationStatus.getOrElse("") == "pending").map(_.socrataId.datasetId)

    expectedApprovedFxfs shouldNot be('empty)
    expectedRejectedFxfs shouldNot be('empty)
    expectedPendingFxfs shouldNot be('empty)

    val userBody = authedUserBodyFromRole("administrator")
    prepareAuthenticatedUser(cookie, moderatedDomain, userBody)

    val params = Map("search_context" -> moderatedDomain, "domains" -> moderatedDomain).mapValues(Seq(_))
    val actualApprovedFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("approved")),
      requireAuth = true, AuthParams(cookie=Some(cookie)), Some(moderatedDomain), None)._2)
    val actualRejectedFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("rejected")),
      requireAuth = true, AuthParams(cookie=Some(cookie)), Some(moderatedDomain), None)._2)
    val actualPendingFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("pending")),
      requireAuth = true, AuthParams(cookie=Some(cookie)), Some(moderatedDomain), None)._2)

    actualApprovedFxfs should contain theSameElementsAs expectedApprovedFxfs
    actualRejectedFxfs should contain theSameElementsAs expectedRejectedFxfs
    actualPendingFxfs should contain theSameElementsAs expectedPendingFxfs
  }

  test("searching as an admin on an unmoderated domain (as both search_context & domain), should find the correct set of views for the given approval_status") {
    // domain 0 has approved/pending/rejected datalenses
    val unmoderatedDomain = domains(0).domainCname
    val allPossibleResults = getAllPossibleResults()
    val domain0Docs = docs.filter(d => d.socrataId.domainId == 0)
    val approvedFxfs = allPossibleResults.filter(isApproved).map(fxfs)
    val datalenses = domain0Docs.filter(d => d.datatype.startsWith("datalens"))

    // approved views are views that pass all 3 types of approval (though in this case, only two are relevant)
    val expectedApprovedFxfs = domain0Docs.filter(d => approvedFxfs.contains(d.socrataId.datasetId)).map(_.socrataId.datasetId)
    // rejected views can only be rejected datalens, since there is no mechanism to reject anything else (TODO: change when introduce RA)
    val expectedRejectedFxfs = datalenses.filter(d => d.moderationStatus.getOrElse("") == "rejected").map(_.socrataId.datasetId)
    // pending views can only be pending datalens, since there is no means for anything else to be pending (TODO: change when introduce RA)
    val expectedPendingFxfs = datalenses.filter(d => d.moderationStatus.getOrElse("") == "pending").map(_.socrataId.datasetId)

    expectedApprovedFxfs shouldNot be('empty)
    expectedRejectedFxfs shouldNot be('empty)
    expectedPendingFxfs shouldNot be('empty)

    val userBody = authedUserBodyFromRole("administrator")
    prepareAuthenticatedUser(cookie, unmoderatedDomain, userBody)

    val params = Map("search_context" -> unmoderatedDomain, "domains" -> unmoderatedDomain).mapValues(Seq(_))
    val actualApprovedFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("approved")),
      requireAuth = true, AuthParams(cookie=Some(cookie)), Some(unmoderatedDomain), None)._2)
    val actualRejectedFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("rejected")),
      requireAuth = true, AuthParams(cookie=Some(cookie)), Some(unmoderatedDomain), None)._2)
    val actualPendingFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("pending")),
      requireAuth = true, AuthParams(cookie=Some(cookie)), Some(unmoderatedDomain), None)._2)

    actualApprovedFxfs should contain theSameElementsAs expectedApprovedFxfs
    actualRejectedFxfs should contain theSameElementsAs expectedRejectedFxfs
    actualPendingFxfs should contain theSameElementsAs expectedPendingFxfs
  }

  test("searching as an admin on an unmoderated domain that federates with a moderated domain, should find the correct set of views for the given approval_status") {
    // domain 0 is an unmoderated domain with approved/pending/rejected datalenses
    // domain 1 is a moderated domain
    val unmoderatedDomain = domains(0).domainCname
    val moderatedDomain = domains(1).domainCname
    val domain0Docs = docs.filter(d => d.socrataId.domainId == 0)
    val domain1Docs = docs.filter(d => d.socrataId.domainId == 1)
    val domain0Datalenses = domain0Docs.filter(d => d.datatype.startsWith("datalens"))
    val allPossibleResults = getAllPossibleResults()
    val approvedFxfs = allPossibleResults.filter(isApproved).map(fxfs)

    // on both 0 and 1, approved views are those that pass all 3 types of approval
    val approvedOn0 = domain0Docs.filter(d => approvedFxfs.contains(d.socrataId.datasetId)).map(_.socrataId.datasetId)
    val approvedOn1 = domain1Docs.filter(d => approvedFxfs.contains(d.socrataId.datasetId)).map(_.socrataId.datasetId)
    val expectedApprovedFxfs = approvedOn0 ++ approvedOn1

    // on 0, b/c it's unmoderated, the only rejected views can be datalenses (TODO: change when RA is include)
    val rejectedOn0 = domain0Datalenses.filter(d => d.moderationStatus.getOrElse("") == "rejected").map(_.socrataId.datasetId)
    // on 1, nothing should come back rejected, b/c an admin on domain 0 doesn't have the rights to see rejected views on domain 1
    val rejectedOn1 = List.empty
    // confirm there are rejected views on domain 1 that could have come back:
    domain1Docs.filter(d => d.moderationStatus.getOrElse("") == "rejected") shouldNot be('empty)
    val expectedRejectedFxfs = rejectedOn0 ++ rejectedOn1

    // on 0, b/c it's unmoderated, the only pending views can be datalenses (TODO: change when RA is include)
    val pendingOn0 = domain0Datalenses.filter(d => d.moderationStatus.getOrElse("") == "pending").map(_.socrataId.datasetId)
    // on 1, nothing should come back pending, b/c an admin on domain 0 doesn't have the rights to see pendings views on domain 1
    val pendingOn1 = List.empty
    // confirm there are pending views on domain 1 that could have come back:
    domain1Docs.filter(d => d.moderationStatus.getOrElse("") == "pending") shouldNot be('empty)
    val expectedPendingFxfs = pendingOn0 ++ pendingOn1

    expectedApprovedFxfs shouldNot be('empty)
    expectedRejectedFxfs shouldNot be('empty)
    expectedPendingFxfs shouldNot be('empty)

    val userBody = authedUserBodyFromRole("administrator")
    prepareAuthenticatedUser(cookie, unmoderatedDomain, userBody)

    val params = Map("search_context" -> Seq(unmoderatedDomain), "domains" -> Seq(s"$moderatedDomain,$unmoderatedDomain"))
    val actualApprovedFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("approved")),
      requireAuth = true, AuthParams(cookie=Some(cookie)), Some(unmoderatedDomain), None)._2)
    val actualRejectedFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("rejected")),
      requireAuth = true, AuthParams(cookie=Some(cookie)), Some(unmoderatedDomain), None)._2)
    val actualPendingFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("pending")),
      requireAuth = true, AuthParams(cookie=Some(cookie)), Some(unmoderatedDomain), None)._2)

    actualApprovedFxfs should contain theSameElementsAs expectedApprovedFxfs
    actualRejectedFxfs should contain theSameElementsAs expectedRejectedFxfs
    actualPendingFxfs should contain theSameElementsAs expectedPendingFxfs
  }

  test("searching as an admin on a moderated domain that federates with an unmoderated domain, should find the correct set of views for the given approval_status") {
    // domain 1 is a moderated domain with approved/pending/rejected views
    // domain 0 is an unmoderated domain with approved/pending/rejected datalenses
    val moderatedDomain = domains(1).domainCname
    val unmoderatedDomain = domains(0).domainCname
    val domain1Docs = docs.filter(d => d.socrataId.domainId == 1)
    val domain0Docs = docs.filter(d => d.socrataId.domainId == 0)
    val allPossibleResults = getAllPossibleResults()
    val approvedFxfs = allPossibleResults.filter(isApproved).map(fxfs)

    // on 1, approved views are those that pass all 3 types of approval
    val approvedOn1 = domain1Docs.filter(d => approvedFxfs.contains(d.socrataId.datasetId)).map(_.socrataId.datasetId)
    // on 0, b/c federating in unmoderated data to a moderated domain removes all derived views, only default views are approved
    val approvedOn0 = domain0Docs.filter(d => approvedFxfs.contains(d.socrataId.datasetId) && d.isDefaultView).map(_.socrataId.datasetId)
    val expectedApprovedFxfs = approvedOn1 ++ approvedOn0

    // on 1, b/c it's moderated, rejected views are just that - rejected views (TODO: change when RA is include)
    val rejectedOn1 = domain1Docs.filter(d => d.moderationStatus.getOrElse("") == "rejected").map(_.socrataId.datasetId)
    // on 0, nothing should come back rejected for 2 reasons:
    //   1) b/c an admin on domain 1 doesn't have the rights to see rejected views on domain 0
    //   2) b/c federating in unmoderated data to a moderated domain removes all derived views
    val rejectedOn0 = List.empty
    // confirm there are rejected views on domain 0 that could have come back:
    domain0Docs.filter(d => d.moderationStatus.getOrElse("") == "rejected") shouldNot be('empty)
    val expectedRejectedFxfs = rejectedOn1 ++ rejectedOn0

    // on 1, b/c it's moderated, pending views are just that - pending views (TODO: change when RA is include)
    val pendingOn1 = domain1Docs.filter(d => d.moderationStatus.getOrElse("") == "pending").map(_.socrataId.datasetId)
    // on 0, nothing should come back pending for 2 reasons:
    //   1) b/c an admin on domain 1 doesn't have the rights to see pending views on domain 0
    //   2) b/c federating in unmoderated data to a moderated domain removes all derived views
    val pendingOn0 = List.empty
    // confirm there are pending views on domain 0 that could have come back:
    domain0Docs.filter(d => d.moderationStatus.getOrElse("") == "pending") shouldNot be('empty)
    val expectedPendingFxfs = pendingOn1 ++ pendingOn0

    expectedApprovedFxfs shouldNot be('empty)
    expectedRejectedFxfs shouldNot be('empty)
    expectedPendingFxfs shouldNot be('empty)

    val userBody = authedUserBodyFromRole("administrator")
    prepareAuthenticatedUser(cookie, moderatedDomain, userBody)

    val params = Map("search_context" -> Seq(moderatedDomain), "domains" -> Seq(s"$moderatedDomain,$unmoderatedDomain"))
    val actualApprovedFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("approved")),
      requireAuth = true, AuthParams(cookie=Some(cookie)), Some(unmoderatedDomain), None)._2)
    val actualRejectedFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("rejected")),
      requireAuth = true, AuthParams(cookie=Some(cookie)), Some(unmoderatedDomain), None)._2)
    val actualPendingFxfs = fxfs(service.doSearch(params ++ Map("approval_status" -> Seq("pending")),
      requireAuth = true, AuthParams(cookie=Some(cookie)), Some(unmoderatedDomain), None)._2)

    actualApprovedFxfs should contain theSameElementsAs expectedApprovedFxfs
    actualRejectedFxfs should contain theSameElementsAs expectedRejectedFxfs
    actualPendingFxfs should contain theSameElementsAs expectedPendingFxfs
  }
}
