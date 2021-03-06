package com.socrata.cetera.services

import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.CompactJsonWriter
import com.socrata.http.server.responses._
import org.mockserver.integration.ClientAndServer._
import org.mockserver.model.HttpRequest._
import org.mockserver.model.HttpResponse._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike, Matchers}

import com.socrata.cetera.auth.AuthParams
import com.socrata.cetera.errors.{DomainNotFoundError, UnauthorizedError}
import com.socrata.cetera.handlers.Params
import com.socrata.cetera.search.{DomainClient, UserClient}
import com.socrata.cetera.types.DomainUser
import com.socrata.cetera.{HeaderAuthorizationKey, HeaderCookieKey, HeaderXSocrataHostKey, TestCoreClient, TestESClient, TestESData, TestHttpClient}

class UserSearchServiceSpec extends FunSuiteLike with Matchers with TestESData
  with BeforeAndAfterAll with BeforeAndAfterEach {

  val userClient = new UserClient(client, testSuiteName)
  val userService = new UserSearchService(userClient, domainClient, coreClient)

  override val cookie = "Traditional = WASD"
  val basicAuth = "Basic cHJvZmVzc29yeDpjZXJlYnJvNGxpZmU="
  val oAuth = "OAuth 123456789"
  val context = Some(domains(0))
  val host = context.get.domainCname
  val adminUserBody = j"""
    {
      "id" : "boo-bear",
      "roleName" : "headBear",
      "rights" : [ "steal_honey", "scare_tourists"],
      "flags" : [ "admin" ]
    }"""

  override protected def beforeAll(): Unit = bootstrapData()

  override def beforeEach(): Unit = mockServer.reset()

  override protected def afterAll(): Unit = {
    removeBootstrapData()
    client.close()
    mockServer.stop(true)
    httpClient.close()
  }

  test("search without authentication throws an unauthorizedError") {
    intercept[UnauthorizedError] {
      userService.doSearch(Map.empty, AuthParams(), None, None)
    }
  }

  test("search with cookie, but no socrata host throws an unauthorizedError") {
    intercept[UnauthorizedError] {
      userService.doSearch(Map.empty, AuthParams(cookie=Some(cookie)), None, None)
    }
  }

  test("search with basic auth, but no socrata host throws an unauthorizedError") {
    val basicAuth = "Basic cHJvZmVzc29yeDpjZXJlYnJvNGxpZmU="
    intercept[UnauthorizedError] {
      userService.doSearch(Map.empty, AuthParams(basicAuth=Some(basicAuth)), None, None)
    }
  }

  test("search with oauth, but no socrata host throws an unauthorizedError") {
    val oAuth = "OAuth cHJvZmVzc29yeDpjZXJlYnJvNGxpZmU="
    intercept[UnauthorizedError] {
      userService.doSearch(Map.empty, AuthParams(oAuth=Some(oAuth)), None, None)
    }
  }

  test("search with authentication but without authorization throws an unauthorizedError") {
    val userBody =
      j"""{
        "id" : "boo-bear",
        "rights" : [ "steal_honey", "scare_tourists"]
        }"""
    val expectedRequest = request()
      .withMethod("GET")
      .withPath("/users.json")
      .withHeader(HeaderXSocrataHostKey, host)
      .withHeader(HeaderCookieKey, cookie)

    mockServer.when(
      expectedRequest
    ).respond(
      response()
        .withStatusCode(200)
        .withHeader("Content-Type", "application/json; charset=utf-8")
        .withBody(CompactJsonWriter.toString(userBody))
    )

    intercept[UnauthorizedError] {
      userService.doSearch(Map.empty, AuthParams(cookie=Some(cookie)), Some(host), None)
    }
  }

  test("search for a domain that doesn't exist throws a DomainNotFoundError") {
    val expectedRequest = request()
      .withMethod("GET")
      .withPath("/users.json")
      .withHeader(HeaderXSocrataHostKey, host)
      .withHeader(HeaderCookieKey, cookie)
    mockServer.when(
      expectedRequest
    ).respond(
      response()
        .withStatusCode(200)
        .withHeader("Content-Type", "application/json; charset=utf-8")
        .withBody(CompactJsonWriter.toString(adminUserBody))
    )

    intercept[DomainNotFoundError] {
      val params = Map(Params.domain -> "bad-domain.com").mapValues(Seq(_))
      userService.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    }
  }

  test("search with superadmin cookie authentication returns any and all users, with required attributes") {
    val expectedRequest = request()
      .withMethod("GET")
      .withPath("/users.json")
      .withHeader(HeaderXSocrataHostKey, host)
      .withHeader(HeaderCookieKey, cookie)
    mockServer.when(
      expectedRequest
    ).respond(
      response()
        .withStatusCode(200)
        .withHeader("Content-Type", "application/json; charset=utf-8")
        .withBody(CompactJsonWriter.toString(adminUserBody))
    )

    val (status, results, _, _) = userService.doSearch(Map.empty, AuthParams(cookie=Some(cookie)), Some(host), None)

    mockServer.verify(expectedRequest)
    status should be(OK)
    results.results.headOption should be('defined)

    val expectedUsers = users.map(u => DomainUser(context, u)).flatten
    results.results should contain theSameElementsAs(expectedUsers)
  }

  test("search with superadmin basic HTTP authentication returns any and all users, with required attributes") {
    val expectedRequest = request()
      .withMethod("GET")
      .withPath("/users.json")
      .withHeader(HeaderXSocrataHostKey, host)
      .withHeader(HeaderAuthorizationKey, basicAuth)
    mockServer.when(
      expectedRequest
    ).respond(
      response()
        .withStatusCode(200)
        .withHeader("Content-Type", "application/json; charset=utf-8")
        .withBody(CompactJsonWriter.toString(adminUserBody))
    )

    val (status, results, _, _) = userService.doSearch(Map.empty, AuthParams(basicAuth=Some(basicAuth)), Some(host), None)

    mockServer.verify(expectedRequest)
    status should be(OK)
    results.results.headOption should be('defined)

    val expectedUsers = users.map(u => DomainUser(context, u)).flatten
    results.results should contain theSameElementsAs(expectedUsers)
  }

  test("search with superadmin OAuth authentication returns any and all users, with required attributes") {
    val expectedRequest = request()
      .withMethod("GET")
      .withPath("/users.json")
      .withHeader(HeaderXSocrataHostKey, host)
      .withHeader(HeaderAuthorizationKey, oAuth)
    mockServer.when(
      expectedRequest
    ).respond(
      response()
        .withStatusCode(200)
        .withHeader("Content-Type", "application/json; charset=utf-8")
        .withBody(CompactJsonWriter.toString(adminUserBody))
    )

    val (status, results, _, _) = userService.doSearch(Map.empty, AuthParams(oAuth=Some(oAuth)), Some(host), None)

    mockServer.verify(expectedRequest)
    status should be(OK)
    results.results.headOption should be('defined)

    val expectedUsers = users.map(u => DomainUser(context, u)).flatten
    results.results should contain theSameElementsAs(expectedUsers)
  }

  test("query search with an email should produce most relevant result first") {
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

    val params = Map(Params.q -> "dark.star@deathcity.com").mapValues(Seq(_))
    val (status, results, _, _) = userService.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    mockServer.verify(expectedRequest)
    status should be(OK)

    results.results.headOption should be('defined)
    val expectedFirstUser = DomainUser(context, users(2)).get
    results.results.head should be(expectedFirstUser)
  }

  test("query search with a screen name should produce most relevant result first") {
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

    val params = Map(Params.q -> "death-the-kid").mapValues(Seq(_))
    val (status, results, _, _) = userService.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    mockServer.verify(expectedRequest)
    status should be(OK)

    results.results.headOption should be('defined)
    val expectedFirstUser = DomainUser(context, users(1)).get
    results.results.head should be(expectedFirstUser)
  }

  test("searching by email and role should produce most relevant result first") {
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

    val params = Map(Params.q -> "dark.star@deathcity.com", Params.roles -> "assasin").mapValues(Seq(_))
    val (status, results, _, _) = userService.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    mockServer.verify(expectedRequest)
    status should be(OK)

    results.results.headOption should be('defined)
    val expectedFirstUser = DomainUser(context, users(2)).get
    results.results.head should be(expectedFirstUser)
  }

  test("search from a given context with no domain param should return roles from the context") {
    val context = domains(2).domainCname
    val expectedRequest = request()
      .withMethod("GET")
      .withPath("/users.json")
      .withHeader(HeaderXSocrataHostKey, context)
      .withHeader(HeaderAuthorizationKey, basicAuth)
    mockServer.when(
      expectedRequest
    ).respond(
      response()
        .withStatusCode(200)
        .withHeader("Content-Type", "application/json; charset=utf-8")
        .withBody(CompactJsonWriter.toString(adminUserBody))
    )

    val (status, results, _, _) = userService.doSearch(Map.empty, AuthParams(basicAuth=Some(basicAuth)), Some(context), None)

    mockServer.verify(expectedRequest)
    status should be(OK)
    results.results.headOption should be('defined)

    val expectedUsers = users.map(u => DomainUser(Some(domains(2)), u)).flatten
    results.results should contain theSameElementsAs(expectedUsers)
    results.results.find(u => u.id == "bright-heart").get.roleName.get should be("racoon")
  }

  test("search from a given context about a different domain should return roles from the domain") {
    val context = domains(2).domainCname
    val domain = domains(1).domainCname
    val expectedRequest = request()
      .withMethod("GET")
      .withPath("/users.json")
      .withHeader(HeaderXSocrataHostKey, context)
      .withHeader(HeaderAuthorizationKey, basicAuth)
    mockServer.when(
      expectedRequest
    ).respond(
      response()
        .withStatusCode(200)
        .withHeader("Content-Type", "application/json; charset=utf-8")
        .withBody(CompactJsonWriter.toString(adminUserBody))
    )

    val params = Map(Params.domain -> domain).mapValues(Seq(_))
    val (status, results, _, _) = userService.doSearch(params, AuthParams(basicAuth=Some(basicAuth)), Some(context), None)

    mockServer.verify(expectedRequest)
    status should be(OK)
    results.results.headOption should be('defined)

    val expectedUsers = Set(users(3), users(4), users(5), users(6)).map(u => DomainUser(Some(domains(1)), u)).flatten
    results.results should contain theSameElementsAs(expectedUsers)
    results.results.find(u => u.id == "bright-heart").get.roleName.get should be("honorary-bear")  // and not "racoon" as is the role on domain 2
  }

  test("searching by email should be case insensitive") {
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

    val params = Map(Params.emails -> "I.heart.Canada@marvel.com").mapValues(Seq(_))
    val (status, results, _, _) = userService.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)

    mockServer.verify(expectedRequest)
    status should be(OK)

    results.results.headOption should be('defined)
    val expectedFirstUser = DomainUser(context, users(6)).get
    results.results.head should be(expectedFirstUser)

    val params2 = Map(Params.emails -> "i.heart.canada@marvel.com").mapValues(Seq(_))
    val (status2, results2, _, _) = userService.doSearch(params, AuthParams(cookie=Some(cookie)), Some(host), None)
    results.results.map(_.id) should contain theSameElementsAs(results2.results.map(_.id))
  }
}
