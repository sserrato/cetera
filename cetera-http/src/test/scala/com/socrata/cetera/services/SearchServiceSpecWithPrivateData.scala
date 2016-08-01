package com.socrata.cetera.services

import java.io.File

import com.rojoma.json.v3.ast.{JBoolean, JString, JValue}
import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.CompactJsonWriter
import com.socrata.http.server.responses._
import org.mockserver.integration.ClientAndServer._
import org.mockserver.model.HttpRequest._
import org.mockserver.model.HttpResponse._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike, Matchers}

import com.socrata.cetera.auth.{AuthParams, VerificationClient}
import com.socrata.cetera.handlers.Params
import com.socrata.cetera.handlers.util.MultiQueryParams
import com.socrata.cetera.metrics.BalboaClient
import com.socrata.cetera.response.{Format, SearchResult, SearchResults}
import com.socrata.cetera.search.{DocumentClient, DomainClient, Visibility}
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
  val verificationClient = new VerificationClient(coreClient)

  val domainClient = new DomainClient(client, coreClient, testSuiteName)
  val documentClient = new DocumentClient(client, domainClient, testSuiteName, None, None, Set.empty)
  val balboaDir = new File("balboa_test_trash")
  val balboaClient = new BalboaClient(balboaDir.getName)

  val service = new SearchService(documentClient, domainClient, balboaClient, verificationClient)

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
          Params.context -> cname,
          Params.filterDomains -> cname
        )
      },
      ownerUserId.map(u => Seq(Params.filterUser -> u)),
      sharedToUserId.map(u => Seq(Params.filterSharedTo -> u))
    ).flatten.flatten.toMap.mapValues(Seq(_))

  private def validateRequest(
      cookie: Option[String], host: Option[String],
      owner: Option[String], sharedTo: Option[String],
      showVisibility: Boolean, visibility: Visibility,
      statusValidator: StatusResponse => Unit,
      resultsValidator: SearchResults[SearchResult] => Unit)
    : Unit = {
    val (responseStatus, responseResults, _, _) =
      service.doSearch(requestParams(host, owner, sharedTo, showVisibility), visibility, AuthParams(cookie=cookie), host, None)
    statusValidator(responseStatus)
    resultsValidator(responseResults)
  }

  private def fxfs(searchResults: SearchResults[SearchResult]): Seq[String] =
    searchResults.results.map(_.resource.dyn.id.!.asInstanceOf[JString].string)

  private def fxfsVisibility(searchResults: SearchResults[SearchResult]): Map[String, Boolean] =
    searchResults.results.map { hit =>
      val fxf = hit.resource.dyn.id.!.asInstanceOf[JString].string
      val visibility = hit.metadata.getOrElse(Format.visibleToAnonKey, fail()).asInstanceOf[JBoolean].boolean
      fxf -> visibility
    }.toMap

  test("searching with full visibility requires auth") {
    validateRequest(
      None, None, None, None, showVisibility = false, Visibility.full,
      _ should be(Unauthorized),
      _.results.headOption should be('empty)
    )
  }

  test("searching with asset selector visibility requires auth") {
    validateRequest(
      None, None, None, None, showVisibility = false, Visibility.assetSelector,
      _ should be(Unauthorized),
      _.results.headOption should be('empty)
    )
  }

  test("searching with personal catalog visibility requires auth") {
    validateRequest(
      None, None, None, None, showVisibility = false, Visibility.personalCatalog,
      _ should be(Unauthorized),
      _.results.headOption should be('empty)
    )
  }

  test("searching with full visibility shows public & private bits") {
    val cookie = "C = Cookie"
    val host = "annabelle.island.net"
    val authedUserBody =
      j"""{
        "id" : "cook-mons",
        "roleName" : "tasteTester",
        "rights" : [ "eat_cookies", "spell_words_starting_with_c" ]
        }"""
    val expectedFxfs = Set("fxf-3", "fxf-7", "zeta-0002", "zeta-0005", "zeta-0009")

    prepareAuthenticatedUser(cookie, host, authedUserBody)
    validateRequest(
      Some(cookie), Some(host), None, None, showVisibility = false, Visibility.full,
      _ should be(OK),
      fxfs(_) should contain theSameElementsAs expectedFxfs
    )
  }

  test("searching with asset selector visibility shows public & private bits") {
    val cookie = "C = Cookie"
    val host = "annabelle.island.net"
    val authedUserBody =
      j"""{
        "id" : "dr-acula",
        "roleName" : "administrator",
        "rights" : [ "bite_necks", "drink_blood" ]
        }"""
    val expectedFxfs = Set("zeta-0002", "zeta-0005", "zeta-0009")

    prepareAuthenticatedUser(cookie, host, authedUserBody)
    validateRequest(
      Some(cookie), Some(host), None, None, showVisibility = false, Visibility.assetSelector,
      _ should be(OK),
      fxfs(_) should contain theSameElementsAs expectedFxfs
    )
  }

  test("searching with asset selector visibility shows public & private & shared bits") {
    val cookie = "C = Cookie"
    val host = "annabelle.island.net"
    val authedUserBody =
      j"""{
        "id" : "lil-john",
        "roleName" : "editor",
        "rights" : [ "walk_though_forest", "laugh_back_and_forth", "reminisce" ]
        }"""
    val expectedFxfs = Set("fxf-3", "fxf-7", "zeta-0002", "zeta-0005", "zeta-0009")

    prepareAuthenticatedUser(cookie, host, authedUserBody)
    validateRequest(
      Some(cookie), Some(host), None, None, showVisibility = false, Visibility.assetSelector,
      _ should be(OK),
      fxfs(_) should contain theSameElementsAs expectedFxfs
    )
  }

  test("when requested, include post-calculated anonymous visibility field") {
    val cookie = "C = Cookie"
    val host = "annabelle.island.net"
    val authedUserBody =
      j"""{
        "id" : "lil-john",
        "roleName" : "editor",
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
      Some(cookie), Some(host), None, None, showVisibility = true, Visibility.full,
      _ should be(OK),
      fxfsVisibility(_) should contain theSameElementsAs expectedFxfs
    )
  }

  test("federated search results also know their own visibility") {
    val cookie = "C = Cookie"
    val host = "petercetera.net"
    val authedUserBody =
      j"""{
        "id" : "cook-mons",
        "roleName" : "tasteTester",
        "rights" : [ "eat_cookies", "spell_words_starting_with_c" ]
        }"""
    val expectedFxfs = Map(
      "fxf-0" -> true,
      "fxf-4" -> true,
      "fxf-8" -> true,
      "zeta-0001" -> true,
      "zeta-0003" -> false,
      "zeta-0004" -> false, // <-- this one is an unapproved datalens
      "zeta-0006" -> false,
      "zeta-0007" -> true
    )

    prepareAuthenticatedUser(cookie, host, authedUserBody)
    validateRequest(
      Some(cookie), Some(host), None, None, showVisibility = true, Visibility.full,
      _ should be(OK),
      fxfsVisibility(_) should contain theSameElementsAs expectedFxfs
    )
  }

  test("searching for assets shared to logged in user returns nothing if nothing is shared") {
    val cookie = "C = Cookie"
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
      Some(cookie), Some(host), None, Some("No One"), showVisibility = false, Visibility.personalCatalog,
      _ should be(OK),
      fxfs(_) should contain theSameElementsAs expectedFxfs
    )
  }

  test("searching for assets shared to logged in user works if a public/published asset is shared") {
    val cookie = "C = Cookie"
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
      Some(cookie), Some(host), None, Some("King Richard"), showVisibility = false, Visibility.personalCatalog,
      _ should be(OK),
      fxfs(_) should contain theSameElementsAs expectedFxfs
    )
  }

  test("searching for assets shared to logged in user works if a private/unpublished asset is shared") {
    val cookie = "C = Cookie"
    val host = "petercetera.net"
    val authedUserBody =
      j"""{
        "id" : "Little John",
        "roleName" : "secondInCommand",
        "rights" : [ "rob_from_the_rich", "give_to_the_poor" ]
        }"""
    val expectedFxfs = Seq("zeta-0003", "zeta-0006")

    prepareAuthenticatedUser(cookie, host, authedUserBody)
    validateRequest(
      Some(cookie), Some(host), None, Some("Little John"), showVisibility = false, Visibility.personalCatalog,
      _ should be(OK),
      fxfs(_) should contain theSameElementsAs expectedFxfs
    )
  }

  test("searching for assets owned by logged in user returns nothing if user owns nothing") {
    val cookie = "C = Cookie"
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
      Some(cookie), Some(host), Some("No One"), None, showVisibility = false, Visibility.personalCatalog,
      _ should be(OK),
      fxfs(_) should contain theSameElementsAs expectedFxfs
    )
  }

  test("searching for assets owned by logged in user returns all assets the user owns, regardless of private/public/approval status") {
    val cookie = "C = Cookie"
    val host = "petercetera.net"
    val authedUserBody =
      j"""{
        "id" : "robin-hood",
        "roleName" : "leader",
        "rights" : [ "rob_from_the_rich", "give_to_the_poor", "get_all_the_glory" ]
        }"""
    val expectedFxfs = Seq("fxf-0", "fxf-4", "fxf-8", "zeta-0001", "zeta-0003", "zeta-0006")

    prepareAuthenticatedUser(cookie, host, authedUserBody)
    validateRequest(
      Some(cookie), Some(host), Some("robin-hood"), None, showVisibility = false, Visibility.personalCatalog,
      _ should be(OK),
      fxfs(_) should contain theSameElementsAs expectedFxfs
    )
  }

  test("searching for assets shared to anyone except logged in user returns unauthorized") {
    val cookie = "C = Cookie"
    val host = "petercetera.net"
    val authedUserBody =
      j"""{
        "id" : "No One",
        "roleName" : "nothing",
        "rights" : [ "nothing" ]
        }"""

    prepareAuthenticatedUser(cookie, host, authedUserBody)
    validateRequest(
      Some(cookie), Some(host), None, Some("Different Person"), showVisibility = false, Visibility.personalCatalog,
      _ should be(Unauthorized),
      _.results.headOption should be('empty)
    )
  }

  test("searching for assets owned by anyone except logged in user via personal catalog returns unauthorized") {
    val cookie = "C = Cookie"
    val host = "petercetera.net"
    val authedUserBody =
      j"""{
        "id" : "No One",
        "roleName" : "nothing",
        "rights" : [ "nothing" ]
        }"""

    prepareAuthenticatedUser(cookie, host, authedUserBody)
    validateRequest(
      Some(cookie), Some(host), Some("Different Person"), None, showVisibility = false, Visibility.personalCatalog,
      _ should be(Unauthorized),
      _.results.headOption should be('empty)
    )
  }

  test("searching for assets by sending for_user and shared_to params returns no results") {
    val cookie = "C = Cookie"
    val host = "petercetera.net"
    val authedUserBody =
      j"""{
        "id" : "robin-hood",
        "roleName" : "leader",
        "rights" : [ "rob_from_the_rich", "give_to_the_poor", "get_all_the_glory" ]
        }"""

    prepareAuthenticatedUser(cookie, host, authedUserBody)
    validateRequest(
      Some(cookie), Some(host), Some("robin-hood"), Some("robin-hood"), showVisibility = false, Visibility.personalCatalog,
      _ should be(OK),
      fxfs(_) should be('empty)
    )
  }
}
