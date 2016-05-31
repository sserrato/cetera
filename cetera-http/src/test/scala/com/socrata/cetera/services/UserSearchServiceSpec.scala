package com.socrata.cetera.services

import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.CompactJsonWriter
import com.socrata.http.server.responses._
import org.mockserver.integration.ClientAndServer._
import org.mockserver.model.HttpRequest._
import org.mockserver.model.HttpResponse._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike, Matchers}

import com.socrata.cetera._
import com.socrata.cetera.search.UserClient

class UserSearchServiceSpec extends FunSuiteLike with Matchers with TestESData
  with BeforeAndAfterAll with BeforeAndAfterEach {

  val httpClient = new TestHttpClient()
  val coreTestPort = 8030
  val mockServer = startClientAndServer(coreTestPort)
  val coreClient = new TestCoreClient(httpClient, coreTestPort)

  val client = new TestESClient(testSuiteName)
  val userClient = new UserClient(client, testSuiteName)
  val service = new UserSearchService(userClient, coreClient)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    bootstrapData()
  }

  override def beforeEach(): Unit = {
    mockServer.reset()
  }

  override protected def afterAll(): Unit = {
    removeBootstrapData()
    client.close()
    mockServer.stop(true)
    httpClient.close()
    super.afterAll()
  }

  test("authenticate rejects anonymous requests") {
    val (auth, _) = service.verifyUserAuthorization(None, None, None)
    auth should be(false)
  }

  test("search without authentication is rejected") {
    val (status, results, _, _) = service.doSearch(Map.empty, None, None, None)
    status should be(Unauthorized)
    results.results.headOption should be('empty)
  }

  test("search with authentication returns any and all users") {
    val cookie = "Traditional = WASD"
    val host = "Superior = ESDF"

    val authedUserBody =
      j"""{
        "id" : "boo-bear",
        "roleName" : "headBear",
        "rights" : [ "steal_honey", "scare_tourists"],
        "flags" : [ "admin" ]
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

    val (status, results, _, _) = service.doSearch(Map.empty, Some(cookie), Some(host), None)

    mockServer.verify(expectedRequest)
    status should be(OK)
    results.results.headOption should be('defined)
  }

  test("search with authentication but without authorization is rejected") {
    val cookie = "Traditional = WASD"
    val host = "Superior = ESDF"

    val authedUserBody =
      j"""{
        "id" : "boo-bear",
        "roleName" : "headBear",
        "rights" : [ "steal_honey", "scare_tourists"]
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

    val (status, results, _, _) = service.doSearch(Map.empty, Some(cookie), Some(host), None)

    mockServer.verify(expectedRequest)
    status should be(Unauthorized)
    results.results.headOption should be('empty)
  }
}
