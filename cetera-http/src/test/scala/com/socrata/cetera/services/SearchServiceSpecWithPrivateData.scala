package com.socrata.cetera.services

import java.io.File

import com.rojoma.json.v3.ast.{JBoolean, JString}
import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.CompactJsonWriter
import com.socrata.http.server.responses._
import org.mockserver.integration.ClientAndServer._
import org.mockserver.model.HttpRequest._
import org.mockserver.model.HttpResponse._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike, Matchers}

import com.socrata.cetera.handlers.Params
import com.socrata.cetera.metrics.BalboaClient
import com.socrata.cetera.response.Format
import com.socrata.cetera.search.{DocumentClient, DomainClient, Visibility}
import com.socrata.cetera.{response => _, _}

class SearchServiceSpecWithPrivateData extends FunSuiteLike with Matchers with TestESData with BeforeAndAfterAll
  with BeforeAndAfterEach{
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

  test("searching with full visibility requires auth") {
    val (status, results, _, _) = service.doSearch(Map.empty, Visibility.full, None, None, None)
    status should be(Unauthorized)
    results.results.headOption should be('empty)
  }

  test("searching with full visibility shows private bits") {
    val cookie = "C = Cookie"
    val host = "annabelle.island.net"

    val authedUserBody =
      j"""{
        "id" : "cook-mons",
        "roleName" : "tasteTester",
        "rights" : [ "eat_cookies", "spell_words_starting_with_c"]
        }"""
    val expectedRequest = request()
      .withMethod("GET")
      .withPath("/users.json")
      .withHeader(HeaderXSocrataHostKey, host)
    mockServer.when(
      expectedRequest
    ).respond(
      response()
        .withStatusCode(200)
        .withHeader("Content-Type", "application/json; charset=utf-8")
        .withBody(CompactJsonWriter.toString(authedUserBody))
    )

    val params = Map(
      Params.context -> host,
      Params.filterDomains -> host
    ).mapValues(Seq(_))
    val expectedFxfs = Set("fxf-3", "fxf-7", "zeta-0002", "zeta-0005")

    val (status, res, _, _) = service.doSearch(params, Visibility.full, Some(cookie), Some(host), None)
    status should be(OK)
    val actualFxfs = res.results.map(_.resource.dyn.id.!.asInstanceOf[JString].string)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("when requested, include post-calculated anonymous visibility field") {
    val cookie = "C = Cookie"
    val host = "annabelle.island.net"

    val authedUserBody =
      j"""{
        "id" : "cook-mons",
        "roleName" : "tasteTester",
        "rights" : [ "eat_cookies", "spell_words_starting_with_c"]
        }"""
    val expectedRequest = request()
      .withMethod("GET")
      .withPath("/users.json")
      .withHeader(HeaderXSocrataHostKey, host)
    mockServer.when(
      expectedRequest
    ).respond(
      response()
        .withStatusCode(200)
        .withHeader("Content-Type", "application/json; charset=utf-8")
        .withBody(CompactJsonWriter.toString(authedUserBody))
    )

    val params = Map(
      Params.context -> host,
      Params.filterDomains -> host,
      Params.showVisibility -> "true"
    ).mapValues(Seq(_))
    val expectedFxfs = Map(
      "fxf-3" -> false,
      "fxf-7" -> false,
      "zeta-0002" -> true,
      "zeta-0005"-> true
    ).map { case (k, v) =>
      JString(k) -> JBoolean(v)
    }

    val (_, res, _, _) = service.doSearch(params, Visibility.full, Some(cookie), Some(host), None)
    val actualFxfs = res.results.map { hit =>
      hit.resource.dyn.id.! -> hit.metadata.getOrElse(Format.visibleToAnonKey, fail())
    }.toMap
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("federated search results also know their own visibility") {
    val cookie = "C = Cookie"
    val host = "petercetera.net"

    val authedUserBody =
      j"""{
        "id" : "cook-mons",
        "roleName" : "tasteTester",
        "rights" : [ "eat_cookies", "spell_words_starting_with_c"]
        }"""
    val expectedRequest = request()
      .withMethod("GET")
      .withPath("/users.json")
      .withHeader(HeaderXSocrataHostKey, host)
    mockServer.when(
      expectedRequest
    ).respond(
      response()
        .withStatusCode(200)
        .withHeader("Content-Type", "application/json; charset=utf-8")
        .withBody(CompactJsonWriter.toString(authedUserBody))
    )

    val params = Map(
      Params.context -> host,
      Params.filterDomains -> host,
      Params.showVisibility -> "true"
    ).mapValues(Seq(_))
    val expectedFxfs = Map(
      "fxf-0" -> true,
      "fxf-4" -> true,
      "fxf-8" -> true,
      "zeta-0001" -> true,
      "zeta-0003"-> false,
      "zeta-0004"-> false, // <-- this one is an unapproved datalens
      "zeta-0006"-> false,
      "zeta-0007"-> true
    ).map { case (k, v) =>
      JString(k) -> JBoolean(v)
    }

    val (_, res, _, _) = service.doSearch(params, Visibility.full, Some(cookie), Some(host), None)
    val actualFxfs = res.results.map { hit =>
      hit.resource.dyn.id.! -> hit.metadata.getOrElse(Format.visibleToAnonKey, fail())
    }.toMap
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("shared_to param returns nothing if nothing is shared") {
    val cookie = "C = Cookie"
    val host = "petercetera.net"

    val authedUserBody =
      j"""{
        "id" : "No One",
        "roleName" : "nothing",
        "rights" : [ "nothing" ]
        }"""
    val expectedRequest = request()
      .withMethod("GET")
      .withPath("/personal_catalog.json")
      .withHeader(HeaderXSocrataHostKey, host)
    mockServer.when(
      expectedRequest
    ).respond(
      response()
        .withStatusCode(200)
        .withHeader("Content-Type", "application/json; charset=utf-8")
        .withBody(CompactJsonWriter.toString(authedUserBody))
    )

    val params = Map(
      Params.context -> host,
      Params.filterDomains -> host,
      Params.context -> host,
      Params.filterSharedTo -> "No One"
    ).mapValues(Seq(_))
    val expectedFxfs = None
    val (_, res, _, _) = service.doSearch(params, Visibility.full, Some(cookie), Some(host), None)
    val actualFxfs = res.results.map(_.resource.dyn.id.!.asInstanceOf[JString].string)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("shared_to param works if a public/published asset is shared") {
    val cookie = "C = Cookie"
    val host = "petercetera.net"

    val authedUserBody =
      j"""{
        "id" : "King Richard",
        "roleName" : "King",
        "rights" : [ "collect_taxes", "wear_crown"]
        }"""
    val expectedRequest = request()
      .withMethod("GET")
      .withPath("/users.json")
      .withHeader(HeaderXSocrataHostKey, host)
    mockServer.when(
      expectedRequest
    ).respond(
      response()
        .withStatusCode(200)
        .withHeader("Content-Type", "application/json; charset=utf-8")
        .withBody(CompactJsonWriter.toString(authedUserBody))
    )

    val params = Map(
      Params.context -> host,
      Params.filterDomains -> host,
      Params.context -> host,
      Params.filterSharedTo -> "King Richard"
    ).mapValues(Seq(_))
    val expectedFxfs = Seq("zeta-0007")
    val (_, res, _, _) = service.doSearch(params, Visibility.full, Some(cookie), Some(host), None)
    val actualFxfs = res.results.map(_.resource.dyn.id.!.asInstanceOf[JString].string)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("shared_to param work if a private/unpublished asset is shared") {
    val cookie = "C = Cookie"
    val host = "petercetera.net"

    val authedUserBody =
      j"""{
        "id" : "Little John",
        "roleName" : "secondInCommand",
        "rights" : [ "rob_from_the_rich", "give_to_the_poor" ]
        }"""
    val expectedRequest = request()
      .withMethod("GET")
      .withPath("/users.json")
      .withHeader(HeaderXSocrataHostKey, host)
    mockServer.when(
      expectedRequest
    ).respond(
      response()
        .withStatusCode(200)
        .withHeader("Content-Type", "application/json; charset=utf-8")
        .withBody(CompactJsonWriter.toString(authedUserBody))
    )

    val params = Map(
      Params.context -> host,
      Params.filterDomains -> host,
      Params.context -> host,
      Params.filterSharedTo -> "Little John"
    ).mapValues(Seq(_))
    val expectedFxfs = Seq("zeta-0003", "zeta-0006")
    val (_, res, _, _) = service.doSearch(params, Visibility.full, Some(cookie), Some(host), None)
    val actualFxfs = res.results.map(_.resource.dyn.id.!.asInstanceOf[JString].string)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("for_user param returns nothing if user owns nothing") {
    val cookie = "C = Cookie"
    val host = "petercetera.net"

    val authedUserBody =
      j"""{
        "id" : "No One",
        "roleName" : "nothing",
        "rights" : [ "nothing" ]
        }"""
    val expectedRequest = request()
      .withMethod("GET")
      .withPath("/users.json")
      .withHeader(HeaderXSocrataHostKey, host)
    mockServer.when(
      expectedRequest
    ).respond(
      response()
        .withStatusCode(200)
        .withHeader("Content-Type", "application/json; charset=utf-8")
        .withBody(CompactJsonWriter.toString(authedUserBody))
    )

    val params = Map(
      Params.context -> host,
      Params.filterDomains -> host,
      Params.context -> host,
      Params.filterUser -> "No One"
    ).mapValues(Seq(_))
    val expectedFxfs = None
    val (_, res, _, _) = service.doSearch(params, Visibility.full, Some(cookie), Some(host), None)
    val actualFxfs = res.results.map(_.resource.dyn.id.!.asInstanceOf[JString].string)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("for_user param shows all assets the user owns, regardless of private/public/approval status") {
    val cookie = "C = Cookie"
    val host = "petercetera.net"

    val authedUserBody =
      j"""{
        "id" : "robin-hood",
        "roleName" : "leader",
        "rights" : [ "rob_from_the_rich", "give_to_the_poor", "get_all_the_glory" ]
        }"""
    val expectedRequest = request()
      .withMethod("GET")
      .withPath("/users.json")
      .withHeader(HeaderXSocrataHostKey, host)
    mockServer.when(
      expectedRequest
    ).respond(
      response()
        .withStatusCode(200)
        .withHeader("Content-Type", "application/json; charset=utf-8")
        .withBody(CompactJsonWriter.toString(authedUserBody))
    )

    val params = Map(
      Params.context -> host,
      Params.filterDomains -> host,
      Params.context -> host,
      Params.filterUser -> "robin-hood"
    ).mapValues(Seq(_))
    val expectedFxfs = Seq("fxf-4", "fxf-8", "fxf-0", "zeta-0001", "zeta-0003", "zeta-0006")
    val (_, res, _, _) = service.doSearch(params, Visibility.full, Some(cookie), Some(host), None)
    val actualFxfs = res.results.map(_.resource.dyn.id.!.asInstanceOf[JString].string)

    actualFxfs should contain theSameElementsAs expectedFxfs
  }
}
