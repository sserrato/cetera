package com.socrata.cetera.services

import javax.servlet.http.HttpServletRequest

import com.socrata.http.server.HttpRequest
import com.socrata.http.server.HttpRequest.AugmentedHttpServletRequest
import org.apache.http.HttpStatus.SC_INTERNAL_SERVER_ERROR
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}
import org.springframework.mock.web.MockHttpServletResponse

import com.socrata.cetera._
import com.socrata.cetera.auth.{AuthParams, VerificationClient}
import com.socrata.cetera.handlers.Params
import com.socrata.cetera.search._
import com.socrata.cetera.types.Count

class DomainCountServiceSpec extends FunSuiteLike with Matchers with BeforeAndAfterAll with TestESData {
  val client = new TestESClient(testSuiteName)
  val httpClient = new TestHttpClient()
  val coreClient = new TestCoreClient(httpClient, 8034)
  val verificationClient = new VerificationClient(coreClient)
  val domainClient = new DomainClient(client, coreClient, testSuiteName)
  val service = new DomainCountService(domainClient, verificationClient)

  override protected def beforeAll(): Unit = {
    bootstrapData()
  }

  override protected def afterAll(): Unit = {
    client.close()
    httpClient.close()
  }

  test("count domains from unmoderated search context") {
    val expectedResults = List(
      Count("annabelle.island.net", 2),
      Count("blue.org", 1),
      Count("opendata-demo.socrata.com", 1),
      Count("petercetera.net", 5))
    val (_, res, _, _) = service.doAggregate(Map(
      Params.context -> "petercetera.net",
      Params.filterDomains -> "petercetera.net,opendata-demo.socrata.com,blue.org,annabelle.island.net")
      .mapValues(Seq(_)), AuthParams(), None, None)
    res.results should contain theSameElementsAs expectedResults
  }

  test("count domains from moderated search context") {
    val expectedResults = List(
      Count("annabelle.island.net", 2),
      Count("blue.org", 0),
      Count("opendata-demo.socrata.com", 1),
      Count("petercetera.net", 2))
    val (_, res, _, _) = service.doAggregate(Map(
      Params.context -> "annabelle.island.net",
      Params.filterDomains -> "petercetera.net,opendata-demo.socrata.com,blue.org,annabelle.island.net")
      .mapValues(Seq(_)), AuthParams(), None, None)
    res.results should contain theSameElementsAs expectedResults
  }

  test("count domains default to include only unlocked customer domains") {
    val expectedResults = List(
      Count("annabelle.island.net", 2),
      Count("blue.org", 1),
      Count("dylan.demo.socrata.com", 0),
      // opendata-demo.socrata.com is not a customer domain, so the domain and all docs should be hidden
      // Count("opendata-demo.socrata.com", 0),
      Count("petercetera.net", 5))
    val (_, res, _, _) = service.doAggregate(Map.empty, AuthParams(), None, None)
    res.results should contain theSameElementsAs expectedResults
  }
}

class DomainCountServiceSpecWithBrokenES extends FunSuiteLike with Matchers with MockFactory {
  //  ES is broken within this class because it's not Bootstrapped
  val testSuiteName = "BrokenES"
  val client = new TestESClient(testSuiteName)
  val httpClient = new TestHttpClient()
  val coreClient = new TestCoreClient(httpClient, 8035)
  val verificationClient = new VerificationClient(coreClient)
  val domainClient = new DomainClient(client, coreClient, testSuiteName)
  val service = new DomainCountService(domainClient, verificationClient)

  test("non fatal exceptions throw friendly error string") {
    val expectedResults = """{"error":"We're sorry. Something went wrong."}"""

    val servReq = mock[HttpServletRequest]
    servReq.expects('getHeader)(HeaderAuthorizationKey).anyNumberOfTimes.returns("Basic ricky:awesome")
    servReq.expects('getHeader)(HeaderCookieKey).anyNumberOfTimes.returns("ricky=awesome")
    servReq.expects('getHeader)(HeaderXSocrataHostKey).anyNumberOfTimes.returns("opendata.test")
    servReq.expects('getHeader)(HeaderXSocrataRequestIdKey).anyNumberOfTimes.returns("1")
    servReq.expects('getQueryString)().returns("only=datasets")

    val augReq = new AugmentedHttpServletRequest(servReq)

    val httpReq = mock[HttpRequest]
    httpReq.expects('servletRequest)().anyNumberOfTimes.returning(augReq)

    val response = new MockHttpServletResponse()

    service.aggregate()(httpReq)(response)
    response.getStatus shouldBe SC_INTERNAL_SERVER_ERROR
    response.getHeader("Access-Control-Allow-Origin") shouldBe "*"
    response.getContentAsString shouldBe expectedResults
  }
}
