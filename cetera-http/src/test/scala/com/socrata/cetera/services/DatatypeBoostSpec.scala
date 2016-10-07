package com.socrata.cetera.services

import com.rojoma.json.v3.ast.JString
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}

import com.socrata.cetera._
import com.socrata.cetera.auth.AuthParams
import com.socrata.cetera.handlers.Params
import com.socrata.cetera.metrics.BalboaClient
import com.socrata.cetera.response.{SearchResult, SearchResults}
import com.socrata.cetera.search._
import com.socrata.cetera.types._

class DatatypeBoostSpec extends FunSuiteLike with Matchers with TestESData with BeforeAndAfterAll {
  val boostedDatatype = TypeCalendars
  val boostedDatatypeQueryString = "boost" + boostedDatatype.plural.capitalize

  val client = new TestESClient(testSuiteName)
  val httpClient = new TestHttpClient()
  val coreClient = new TestCoreClient(httpClient, 8033)
  val domainClient = new DomainClient(client, coreClient, testSuiteName)
  val documentClient = new DocumentClient(client, domainClient, testSuiteName, None, None, Set.empty)
  val balboaClient = new BalboaClient("/tmp/metrics")
  val service = new SearchService(documentClient, domainClient, balboaClient, coreClient)

  override protected def beforeAll(): Unit = {
    bootstrapData()
  }

  override protected def afterAll(): Unit = {
    removeBootstrapData()
    client.close()
    httpClient.close()
  }

  private def constructQueryParams(query: QueryType): Map[String, Seq[String]] = {
    val queryParam = query match {
        case NoQuery => None
        case SimpleQuery(q) => Some(Params.querySimple -> q)
        case AdvancedQuery(q) => Some(Params.queryAdvanced -> q)
      }

    val params = Map(
      Params.context -> "petercetera.net",
      Params.filterDomains -> "petercetera.net,opendata-demo.socrata.com,blue.org,annabelle.island.net",
      Params.showScore -> "true",
      boostedDatatypeQueryString -> "10"
    ) ++ queryParam

    params.mapValues(Seq(_))
  }

  private def extractBoostedAndAnyOtherScore(results: SearchResults[SearchResult]): (BigDecimal, BigDecimal) = {
    val oneBoosted = results.results.find(_.resource.dyn.`type`.! == JString(boostedDatatype.singular)).head
    val oneOtherThing = results.results.find(_.resource.dyn.`type`.! != JString(boostedDatatype.singular)).head

    def extractScore(result: SearchResult): BigDecimal = {
      result.metadata.score.getOrElse(fail())
    }

    (extractScore(oneBoosted), extractScore(oneOtherThing))
  }

  test("simple query - increases score when datatype matches") {
    val (_, results, _, _) = service.doSearch(constructQueryParams(SimpleQuery("one")), false, AuthParams(), None, None)
    val (boostedScore, otherScore) = extractBoostedAndAnyOtherScore(results)
    boostedScore should be > otherScore
  }

  test("no query - increases score when datatype matches") {
    val (_, results, _, _) = service.doSearch(constructQueryParams(NoQuery), false, AuthParams(), None, None)
    val (boostedScore, otherScore) = extractBoostedAndAnyOtherScore(results)
    boostedScore should be > otherScore
  }

  test("advanced query - increases score when datatype matches") {
    val (_, results, _, _) = service.doSearch(constructQueryParams(AdvancedQuery("one OR two OR three OR four")), false, AuthParams(), None, None)
    val (boostedScore, otherScore) = extractBoostedAndAnyOtherScore(results)
    boostedScore should be > otherScore
  }
}
