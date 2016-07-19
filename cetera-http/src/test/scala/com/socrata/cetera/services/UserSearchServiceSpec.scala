package com.socrata.cetera.services

import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.CompactJsonWriter
import com.socrata.http.server.responses._
import org.mockserver.integration.ClientAndServer._
import org.mockserver.model.HttpRequest._
import org.mockserver.model.HttpResponse._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike, Matchers}

import com.socrata.cetera.{TestCoreClient, TestESClient, TestESData, TestHttpClient}
import com.socrata.cetera.search.{DomainClient, UserClient}
import com.socrata.cetera.HeaderXSocrataHostKey
import com.socrata.cetera.auth.VerificationClient
import com.socrata.cetera.handlers.Params
import com.socrata.cetera.types.DomainUser

class UserSearchServiceSpec extends FunSuiteLike with Matchers with TestESData
  with BeforeAndAfterAll with BeforeAndAfterEach {

  val httpClient = new TestHttpClient()
  val coreTestPort = 8031
  val mockServer = startClientAndServer(coreTestPort)
  val coreClient = new TestCoreClient(httpClient, coreTestPort)
  val verificationClient = new VerificationClient(coreClient)

  val client = new TestESClient(testSuiteName)
  val domainClient = new DomainClient(client, coreClient, testSuiteName)
  val userClient = new UserClient(client, testSuiteName)
  val service = new UserSearchService(userClient, verificationClient, domainClient)

  val cookie = "Traditional = WASD"
  val context = Some(domains(0))
  val host = context.get.domainCname
  val adminUserBody = j"""
    {
      "id" : "boo-bear",
      "roleName" : "headBear",
      "rights" : [ "steal_honey", "scare_tourists"],
      "flags" : [ "admin" ]
    }"""

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

  test("search without authentication is rejected") {
    val (status, results, _, _) = service.doSearch(Map.empty, None, None, None)
    status should be(Unauthorized)
    results.results.headOption should be('empty)
  }

  test("search without domain is rejected") {
    val cookie = "Traditional = WASD"
    val (status, results, _, _) = service.doSearch(Map.empty, Some(cookie), None, None)
    status should be(Unauthorized)
    results.results.headOption should be('empty)
  }

  test("search with authentication returns any and all users, with required attributes") {
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
        .withBody(CompactJsonWriter.toString(adminUserBody))
    )

    val (status, results, _, _) = service.doSearch(Map.empty, Some(cookie), Some(host), None)

    mockServer.verify(expectedRequest)
    status should be(OK)
    results.results.headOption should be('defined)

    val expectedUsers = users.map(u => DomainUser(context, u)).flatten
    results.results should contain theSameElementsAs(expectedUsers)
  }

  test("search with authentication but without authorization is rejected") {
    val userBody =
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
        .withBody(CompactJsonWriter.toString(userBody))
    )

    val (status, results, _, _) = service.doSearch(Map.empty, Some(cookie), Some(host), None)

    mockServer.verify(expectedRequest)
    status should be(Unauthorized)
    results.results.headOption should be('empty)
  }

  test("email search should produce most relevant result first") {
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
        .withBody(CompactJsonWriter.toString(adminUserBody))
    )

    val params = Map(Params.querySimple -> "dark.star@deathcity.com").mapValues(Seq(_))
    val (status, results, _, _) = service.doSearch(params, Some(cookie), Some(host), None)
    mockServer.verify(expectedRequest)
    status should be(OK)

    results.results.headOption should be('defined)
    val expectedFirstUser = DomainUser(context, users(2)).get
    results.results.head should be(expectedFirstUser)
  }

  test("screen name search should produce most relevant result first") {
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
        .withBody(CompactJsonWriter.toString(adminUserBody))
    )

    val params = Map(Params.querySimple -> "death-the-kid").mapValues(Seq(_))
    val (status, results, _, _) = service.doSearch(params, Some(cookie), Some(host), None)
    mockServer.verify(expectedRequest)
    status should be(OK)

    results.results.headOption should be('defined)
    val expectedFirstUser = DomainUser(context, users(1)).get
    results.results.head should be(expectedFirstUser)
  }
}
