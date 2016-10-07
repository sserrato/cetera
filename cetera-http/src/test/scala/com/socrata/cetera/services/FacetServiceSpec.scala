package com.socrata.cetera.services

import javax.servlet.http.HttpServletRequest
import scala.collection.mutable.ArrayBuffer

import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.CompactJsonWriter
import com.socrata.http.server.HttpRequest
import com.socrata.http.server.HttpRequest.AugmentedHttpServletRequest
import org.apache.http.HttpStatus.SC_INTERNAL_SERVER_ERROR
import org.mockserver.integration.ClientAndServer._
import org.mockserver.matchers.Times
import org.mockserver.model.HttpRequest.request
import org.mockserver.model.HttpResponse.response
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike, Matchers}
import org.springframework.mock.web.MockHttpServletResponse

import com.socrata.cetera._
import com.socrata.cetera.auth.AuthParams
import com.socrata.cetera.errors.{DomainNotFoundError, UnauthorizedError}
import com.socrata.cetera.search._
import com.socrata.cetera.types.{FacetCount, ValueCount}

class FacetServiceSpec
  extends FunSuiteLike
  with Matchers
  with TestESData
  with BeforeAndAfterAll
  with BeforeAndAfterEach {

  val client = new TestESClient(testSuiteName)
  val httpClient = new TestHttpClient()
  val coreTestPort = 8035
  val coreClient = new TestCoreClient(httpClient, coreTestPort)
  val domainClient = new DomainClient(client, coreClient, testSuiteName)
  val documentClient = new DocumentClient(client, domainClient, testSuiteName, None, None, Set.empty)
  val service = new FacetService(documentClient, domainClient, coreClient)
  val mockServer = startClientAndServer(coreTestPort)

  override def beforeEach(): Unit = {
    mockServer.reset()
  }

  override protected def beforeAll(): Unit = {
    bootstrapData()
  }

  override protected def afterAll(): Unit = {
    removeBootstrapData()
    client.close()
    mockServer.stop(true)
    httpClient.close()
  }

  private def parseValues(f: Option[FacetCount]): Seq[ValueCount] = {
    f.collect { case fc: FacetCount if (fc.count > 0) => fc.values}.getOrElse(Seq.empty)
  }

  case class FacetRes(
      tags: Seq[ValueCount],
      categories: Seq[ValueCount],
      datatypes: Seq[ValueCount],
      metadata: Seq[ValueCount])

  test("retrieve all visible domain facets for domains that are unlocked") {
    val domainResults = List(
      service.doAggregate(domains(0).domainCname, AuthParams(), None, None),
      service.doAggregate(domains(1).domainCname, AuthParams(), None, None),
      service.doAggregate(domains(2).domainCname, AuthParams(), None, None),
      service.doAggregate(domains(3).domainCname, AuthParams(), None, None),
      service.doAggregate(domains(4).domainCname, AuthParams(), None, None),
      service.doAggregate(domains(5).domainCname, AuthParams(), None, None))

    //val (datatypes, categories, tags, facets) =
    val domainFacets = domainResults.map{ r =>
      val facets = r._2
      val tags = parseValues(facets.find(_.facet == "tags"))
      val categories = parseValues(facets.find(_.facet == "categories"))
      val datatypes = parseValues(facets.find(_.facet == "datatypes"))
      val metadata = facets.filter(f => !List("datatypes", "categories", "tags").contains(f.facet)).map(f => parseValues(Some(f))).flatten
      FacetRes(tags, categories, datatypes, metadata)
    }

    // domain 0 has 2 datasets (zeta-0001 and zeta-0007), a calendar (fxf-0), a file (fxf-4) and a href (fxf-8) that are anonymously viewable
    domainFacets(0).datatypes should contain theSameElementsAs(List(ValueCount("dataset", 2), ValueCount("calendar",1),
      ValueCount("file",1), ValueCount("href",1)))
    // domain 0 has 3 views with the "alpha to omega" category and 2 with "Fun"
    domainFacets(0).categories should contain theSameElementsAs(List(ValueCount("Alpha to Omega",3), ValueCount("Fun",2)))
    // domain 0 has 3 views with the "1-one" tag and 1 with "2-two"
    domainFacets(0).tags should contain theSameElementsAs(List(ValueCount("1-one",3), ValueCount("2-two",1)))
    // domain 0 has 1 view with the "8" metadata value
    domainFacets(0).metadata should contain theSameElementsAs(List(ValueCount("8",3), ValueCount("1",3), ValueCount("3",3)))

    // domain 1 has 1 chart (fxf-1) that is anonymously viewable
    domainFacets(1).datatypes should contain theSameElementsAs(List(ValueCount("chart", 1)))
    // it has the "Beta" category
    domainFacets(1).categories should contain theSameElementsAs(List(ValueCount("Beta",1)))
    // and two tags: "1-one" and "2-two"
    domainFacets(1).tags should contain theSameElementsAs(List(ValueCount("1-one",1), ValueCount("2-two",1)))
    // and the "2" custom metadata value
    domainFacets(1).metadata should contain theSameElementsAs(List(ValueCount("2",1)))

    // domain 2 has 1 story (fxf-10) that is anonymously viewable
    domainFacets(2).datatypes should contain theSameElementsAs(List(ValueCount("story", 1)))
    // it has the "Gamma" category
    domainFacets(2).categories should contain theSameElementsAs(List(ValueCount("Gamma",1)))
    // and two tags: "1-one" and "2-two"
    domainFacets(2).tags should contain theSameElementsAs(List(ValueCount("1-one",1), ValueCount("2-two",1)))
    // and the "3" custom metadata value
    domainFacets(2).metadata should contain theSameElementsAs(List(ValueCount("3",1)))

    // domain 3 has 2 datasets (zeta-2 and zeta-5) that are anonymously viewable
    domainFacets(3).datatypes should contain theSameElementsAs(List(ValueCount("dataset", 2)))
    // these both have the "Fun" category
    domainFacets(3).categories should contain theSameElementsAs(List(ValueCount("Fun",2)))
    // and neither have tags or metadata
    domainFacets(3).tags should be('empty)
    domainFacets(3).metadata should be('empty)

    // domain 4 has nothing on it
    domainFacets(4).datatypes should be('empty)
    domainFacets(4).categories should be('empty)
    domainFacets(4).tags should be('empty)
    domainFacets(4).metadata should be('empty)

    // domain 5 has nothing on it
    domainFacets(5).datatypes should be('empty)
    domainFacets(5).categories should be('empty)
    domainFacets(5).tags should be('empty)
    domainFacets(5).metadata should be('empty)
  }

  test("retrieve all visible domain facets on a locked domain if user is authed properly") {
    val context = domains(8)
    val userBody = j"""{ "id" : "boo-bear", "roleName" : "editor" }"""

    mockServer.when(
      request()
        .withMethod("GET")
        .withPath("/users.json")
        .withHeader(HeaderXSocrataHostKey, context.domainCname)
    ).respond(
      response()
        .withStatusCode(200)
        .withHeader("Content-Type", "application/json; charset=utf-8")
        .withBody(CompactJsonWriter.toString(userBody))
    )
    mockServer.when(
      request()
        .withMethod("GET")
        .withPath("/users/boo-bear")
    ).respond(
      response()
        .withStatusCode(200)
        .withHeader("Content-Type", "application/json; charset=utf-8")
        .withBody(CompactJsonWriter.toString(userBody))
    )

    val (_, facets, timings, _) = service.doAggregate(context.domainCname, AuthParams(cookie=Some("c=cookie")), Some(context.domainCname), None)
    val datatypes = facets.find(_.facet == "datatypes").map(_.values).getOrElse(fail())
    val categories = facets.find(_.facet == "categories").map(_.values).getOrElse(fail())
    val tags = facets.find(_.facet == "tags").map(_.values).getOrElse(fail())

    val expectedDatatypes = List(ValueCount("datalens", 1))
    datatypes should contain theSameElementsAs expectedDatatypes

    val expectedCategories = List(ValueCount("Fun", 1))
    categories should contain theSameElementsAs expectedCategories

    val expectedTags = List(ValueCount("fake", 1), ValueCount("king", 1))
    tags should contain theSameElementsAs expectedTags

    val expectedFacets = List(
      FacetCount("datatypes", 1, ArrayBuffer(ValueCount("datalens", 1))),
      FacetCount("categories", 1, ArrayBuffer(ValueCount("Fun", 1))),
      FacetCount("tags", 2, ArrayBuffer(ValueCount("fake", 1), ValueCount("king", 1))))

    facets should contain theSameElementsAs expectedFacets
    facets.foreach(f => f.count should be(f.values.map(_.count).sum))
  }

  test("throws a DomainNotFoundError when the cname doesn't exist") {
    val context = domains(0)
    val userBody = j"""{ "id" : "boo-bear", "roleName" : "editor" }"""

    mockServer.when(
      request()
        .withMethod("GET")
        .withPath("/users.json")
        .withHeader(HeaderXSocrataHostKey, context.domainCname),
      Times.exactly(1)
    ).respond(
      response()
        .withStatusCode(200)
        .withHeader("Content-Type", "application/json; charset=utf-8")
        .withBody(CompactJsonWriter.toString(userBody))
    )
    mockServer.when(
      request()
        .withMethod("GET")
        .withPath("/users/boo-bear"),
      Times.exactly(1)
    ).respond(
      response()
        .withStatusCode(200)
        .withHeader("Content-Type", "application/json; charset=utf-8")
        .withBody(CompactJsonWriter.toString(userBody))
    )

    intercept[DomainNotFoundError] {
      service.doAggregate("bs-domain.com", AuthParams(cookie=Some("c=cookie")), Some(context.domainCname), None)
    }
  }

  test("throw an unauthorizedError if user is not authed properly") {
    val context = domains(8)
    val userBody = j"""{ "id" : "boo-bear"}"""   // poor boo-bear has no role.

    mockServer.when(
      request()
        .withMethod("GET")
        .withPath("/users.json")
        .withHeader(HeaderXSocrataHostKey, context.domainCname),
      Times.exactly(1)
    ).respond(
      response()
        .withStatusCode(200)
        .withHeader("Content-Type", "application/json; charset=utf-8")
        .withBody(CompactJsonWriter.toString(userBody))
    )
    mockServer.when(
      request()
        .withMethod("GET")
        .withPath("/users/boo-bear"),
      Times.exactly(1)
    ).respond(
      response()
        .withStatusCode(200)
        .withHeader("Content-Type", "application/json; charset=utf-8")
        .withBody(CompactJsonWriter.toString(userBody))
    )

    intercept[UnauthorizedError] {
      service.doAggregate(context.domainCname, AuthParams(cookie=Some("c=cookie")), Some(context.domainCname), None)
    }
  }
}

class FacetServiceSpecWithBrokenES extends FunSuiteLike with Matchers with MockFactory {
  //  ES is broken within this class because it's not Bootstrapped
  val testSuiteName = "BrokenES"
  val client = new TestESClient(testSuiteName)
  val httpClient = new TestHttpClient()
  val coreClient = new TestCoreClient(httpClient, 8036)
  val domainClient = new DomainClient(client, coreClient, testSuiteName)
  val documentClient = new DocumentClient(client, domainClient, testSuiteName, None, None, Set.empty)
  val service = new FacetService(documentClient, domainClient, coreClient)

  test("non fatal exceptions throw friendly error string") {
    val expectedResults = """{"error":"We're sorry. Something went wrong."}"""

    val servReq = mock[HttpServletRequest]
    servReq.expects('getHeader)(HeaderAuthorizationKey).anyNumberOfTimes.returns("Basic ricky:awesome")
    servReq.expects('getHeader)(HeaderAuthorizationKey).anyNumberOfTimes.returns("OAuth 123456789")
    servReq.expects('getHeader)(HeaderCookieKey).anyNumberOfTimes.returns("ricky=awesome")
    servReq.expects('getHeader)(HeaderXSocrataHostKey).anyNumberOfTimes.returns("opendata.test")
    servReq.expects('getHeader)(HeaderXSocrataRequestIdKey).anyNumberOfTimes.returns("1")
    servReq.expects('getQueryString)().returns("only=datasets")

    val augReq = new AugmentedHttpServletRequest(servReq)

    val httpReq = mock[HttpRequest]
    httpReq.expects('servletRequest)().anyNumberOfTimes.returning(augReq)

    val response = new MockHttpServletResponse()

    val cname = "something.com"

    service.aggregate(cname)(httpReq)(response)
    response.getStatus shouldBe SC_INTERNAL_SERVER_ERROR
    response.getHeader("Access-Control-Allow-Origin") shouldBe "*"
    response.getContentAsString shouldBe expectedResults
  }
}
