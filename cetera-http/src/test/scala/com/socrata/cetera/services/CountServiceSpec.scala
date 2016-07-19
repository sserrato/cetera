package com.socrata.cetera.services

import javax.servlet.http.HttpServletRequest

import com.rojoma.json.v3.ast.{JNumber, JString}
import com.rojoma.json.v3.codec.DecodeError
import com.rojoma.json.v3.interpolation._
import com.socrata.http.server.HttpRequest
import com.socrata.http.server.HttpRequest.AugmentedHttpServletRequest
import org.apache.http.HttpStatus.SC_INTERNAL_SERVER_ERROR
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}
import org.springframework.mock.web.MockHttpServletResponse

import com.socrata.cetera._
import com.socrata.cetera.auth.VerificationClient
import com.socrata.cetera.response.SearchResults
import com.socrata.cetera.search._
import com.socrata.cetera.types._

class CountServiceSpec extends FunSuiteLike with Matchers with BeforeAndAfterAll {
  val testSuiteName = getClass.getSimpleName.toLowerCase
  val esClient = new TestESClient(testSuiteName)
  val httpClient = new TestHttpClient()
  val coreClient = new TestCoreClient(httpClient, 8031)
  val verificationClient = new VerificationClient(coreClient)
  val domainClient = new DomainClient(esClient, coreClient, testSuiteName)
  val documentClient = new DocumentClient(esClient, domainClient, testSuiteName, None, None, Set.empty)
  val service = new CountService(documentClient, domainClient, verificationClient)

  override protected def afterAll(): Unit = {
    esClient.close() // Important!!
    httpClient.close()
  }

  val esResponse = j"""{
    "took" : 1,
    "timed_out" : false,
    "_shards" : {
      "total" : 10,
      "successful" : 10,
      "failed" : 0
    },
    "hits" : {
      "total" : 1862,
      "max_score" : 0.0,
      "hits" : [ ]
    },
    "aggregations" : {
      "domains" : {
        "doc_count_error_upper_bound" : 0,
        "sum_other_doc_count" : 0,
        "buckets" : [ {
          "key" : "onethousand.example.com",
          "doc_count" : 1000
        }, {
          "key" : "two-thirty-four.example.com",
          "doc_count" : 234
        }, {
          "key" : "seven-ate-nine.com",
          "doc_count" : 78
        }, {
          "key" : "poor-bono.example.com",
          "doc_count" : 1
        } ]
      }
    }
  }"""

  test("extract") {
    val expected = Stream(
      j"""{ "key" : "onethousand.example.com", "doc_count" : 1000 }""",
      j"""{ "key" : "two-thirty-four.example.com", "doc_count" : 234 }""",
      j"""{ "key" : "seven-ate-nine.com", "doc_count" : 78 }""",
      j"""{ "key" : "poor-bono.example.com", "doc_count" : 1 }"""
    )

    service.extract(esResponse) match {
      case Right(actual) =>
        (actual, expected).zipped.foreach{ (a, e) => a should be(e) }

      case Left(e) =>
        fail(e.toString)
    }
  }

  test("format") {
    val expected = SearchResults[Count](
      List(
        Count(JString("onethousand.example.com"), JNumber(1000)),
        Count(JString("two-thirty-four.example.com"),  JNumber(234)),
        Count(JString("seven-ate-nine.com"),  JNumber(78)),
        Count(JString("poor-bono.example.com"),  JNumber(1))
      ), 4
    )

    service.extract(esResponse) match {
      case Right(extracted) =>
        val formatted = service.format(extracted)
        val results = formatted.results
        results.zip(expected.results).foreach{ case (a, e) => a should be (e) }

      case Left(e) =>
        fail(e.toString)
    }
  }

  test("return an error when the expected path to resources does not exist") {
    val body = j"""{}"""
    service.extract(body) match {
      case Right(aggregations) => fail("We should have returned a decode error!")
      case Left(error) => error match {
        case _: DecodeError =>
        case _ => fail("Expected a DecodeError")
      }
    }
  }

  ignore("timings") {}
  ignore("query parameters parser - errors") {}

}


class CountServiceSpecWithTestESData extends FunSuiteLike with Matchers with BeforeAndAfterAll with TestESData {
  val client: ElasticSearchClient = new TestESClient(testSuiteName)
  val httpClient = new TestHttpClient()
  val coreClient = new TestCoreClient(httpClient, 8032)
  val verificationClient = new VerificationClient(coreClient)
  val domainClient = new DomainClient(client, coreClient, testSuiteName)
  val documentClient = new DocumentClient(client, domainClient, testSuiteName, None, None, Set.empty)
  val service = new CountService(documentClient, domainClient, verificationClient)

  override protected def beforeAll(): Unit = {
    bootstrapData()
  }

  override protected def afterAll(): Unit = {
    client.close()
    httpClient.close()
  }

  test("categories count request") {
    val expectedResults = List(Count("Personal", 4))
    val (_, res, _, _) = service.doAggregate(CategoriesFieldType, Map.empty, None, None, None)
    res.results should contain theSameElementsAs expectedResults
  }

  test("tags count request") {
    val expectedResults = List(Count("Happy", 4), Count("Accident", 4))
    val (_, res, _, _) = service.doAggregate(TagsFieldType, Map.empty, None, None, None)
    res.results should contain theSameElementsAs expectedResults
  }

  test("domain categories count request") {
    val expectedResults = List(Count("Alpha to Omega", 3), Count("Gamma", 1), Count("Fun", 4))
    val (_, res, _, _) = service.doAggregate(DomainCategoryFieldType, Map.empty, None, None, None)
    res.results should contain theSameElementsAs expectedResults
  }

  test("domain tags count request") {
    val expectedResults = List(Count("1-one", 3), Count("3-three", 1))
    val (_, res, _, _) = service.doAggregate(DomainTagsFieldType, Map.empty, None, None, None)
    res.results should contain theSameElementsAs expectedResults
  }

  test("owners count request") {
    val expectedResults = List(Count("robin-hood", 5), Count("lil-john", 2),  Count("john-clan", 1))
    val (_, res, _, _) = service.doAggregate(OwnerIdFieldType, Map.empty, None, None, None)
    res.results should contain theSameElementsAs expectedResults
  }
}

class CountServiceSpecWithBrokenES extends FunSuiteLike with Matchers with MockFactory {
//  ES is broken within this class because it's not Bootstrapped
  val testSuiteName = "BrokenES"
  val client: ElasticSearchClient = new TestESClient(testSuiteName)
  val httpClient = new TestHttpClient()
  val coreClient = new TestCoreClient(httpClient, 8033)
  val verificationClient = new VerificationClient(coreClient)
  val domainClient = new DomainClient(client, coreClient, testSuiteName)
  val documentClient = new DocumentClient(client, domainClient, testSuiteName, None, None, Set.empty)
  val service = new CountService(documentClient, domainClient, verificationClient)

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

    val field = DomainCategoryFieldType

    service.aggregate(field)(httpReq)(response)
    response.getStatus shouldBe SC_INTERNAL_SERVER_ERROR
    response.getHeader("Access-Control-Allow-Origin") shouldBe "*"
    response.getContentAsString shouldBe expectedResults
  }
}
