package com.socrata.cetera.services

import javax.servlet.http.HttpServletRequest
import scala.collection.mutable.ArrayBuffer

import com.socrata.http.server.HttpRequest
import com.socrata.http.server.HttpRequest.AugmentedHttpServletRequest
import org.apache.http.HttpStatus.SC_INTERNAL_SERVER_ERROR
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}
import org.springframework.mock.web.MockHttpServletResponse

import com.socrata.cetera._
import com.socrata.cetera.search._
import com.socrata.cetera.types.{FacetCount, ValueCount}

class FacetServiceSpec extends FunSuiteLike with Matchers with TestESData with BeforeAndAfterAll {
  val client = new TestESClient(testSuiteName)
  val httpClient = new TestHttpClient()
  val coreClient = new TestCoreClient(httpClient, 8035)
  val domainClient = new DomainClient(client, coreClient, testSuiteName)
  val documentClient = new DocumentClient(client, domainClient, testSuiteName, None, None, Set.empty)
  val service = new FacetService(documentClient, domainClient)

  override protected def beforeAll(): Unit = {
    bootstrapData()
  }

  override protected def afterAll(): Unit = {
    removeBootstrapData()
    client.close()
    httpClient.close()
  }

  test("retrieve all visible domain facets") {
    val (datatypes, categories, tags, facets) = domainsWithData.map { cname =>
      val (facets, timings, _) = service.doAggregate(cname, None, None)
      timings.searchMillis.headOption should be('defined)

      val datatypes = facets.find(_.facet == "datatypes").map(_.values).getOrElse(fail())
      val categories = facets.find(_.facet == "categories").map(_.values).getOrElse(fail())
      val tags = facets.find(_.facet == "tags").map(_.values).getOrElse(fail())
      (datatypes, categories, tags, facets)
    }.foldLeft((Seq.empty[ValueCount], Seq.empty[ValueCount], Seq.empty[ValueCount], Seq.empty[FacetCount])) {
      (b, x) => (b._1 ++ x._1, b._2 ++ x._2, b._3 ++ x._3, b._4 ++ x._4)
    }

    val expectedDatatypes = List(
      ValueCount("calendar", 1),
      ValueCount("dataset", 2),
      ValueCount("file", 1),
      ValueCount("href", 1),
      ValueCount("chart", 1),
      ValueCount("story", 1),
      ValueCount("dataset", 1))
    datatypes should contain theSameElementsAs expectedDatatypes

    val expectedCategories = List(
      ValueCount("Alpha to Omega", 3),
      ValueCount("Fun", 2),
      ValueCount("Beta", 1),
      ValueCount("Gamma", 1),
      ValueCount("Fun", 1))
    categories.find(_.value == "") shouldNot be('defined)
    categories should contain theSameElementsAs expectedCategories

    val expectedTags = List(
      ValueCount("1-one", 3),
      ValueCount("2-two", 1),
      ValueCount("3-three", 1))
    tags.find(_.value == "") shouldNot be('defined)
    tags should contain theSameElementsAs expectedTags

    val expectedFacets = List(
      FacetCount("datatypes", 4, ArrayBuffer(ValueCount("calendar", 1), ValueCount("dataset", 1), ValueCount("file", 1), ValueCount("href", 1))),
      FacetCount("categories", 4, ArrayBuffer(ValueCount("Alpha to Omega", 3), ValueCount("Fun", 1))),
      FacetCount("tags", 3, ArrayBuffer(ValueCount("1-one", 3))),
      FacetCount("five", 3, ArrayBuffer(ValueCount("8", 3))),
      FacetCount("one", 3, ArrayBuffer(ValueCount("1", 3))),
      FacetCount("two", 3, ArrayBuffer(ValueCount("3", 3))),
      FacetCount("datatypes", 1, ArrayBuffer(ValueCount("chart", 1))),
      FacetCount("categories", 1, ArrayBuffer(ValueCount("Beta", 1))),
      FacetCount("tags", 1, ArrayBuffer(ValueCount("2-two", 1))),
      FacetCount("one", 1, ArrayBuffer(ValueCount("2", 1))),
      FacetCount("datatypes", 1, ArrayBuffer(ValueCount("story", 1))),
      FacetCount("categories", 1, ArrayBuffer(ValueCount("Gamma", 1))),
      FacetCount("tags", 1, ArrayBuffer(ValueCount("3-three", 1))),
      FacetCount("two", 1, ArrayBuffer(ValueCount("3", 1))),
      FacetCount("datatypes", 2, ArrayBuffer(ValueCount("dataset", 2))),
      FacetCount("categories", 2, ArrayBuffer(ValueCount("Fun", 2))),
      FacetCount("tags", 0, ArrayBuffer()))
    facets should contain theSameElementsAs expectedFacets
    facets.foreach(f => f.count should be(f.values.map(_.count).sum))
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
  val service = new FacetService(documentClient, domainClient)

  test("non fatal exceptions throw friendly error string") {
    val expectedResults = """{"error":"We're sorry. Something went wrong."}"""

    val servReq = mock[HttpServletRequest]
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
