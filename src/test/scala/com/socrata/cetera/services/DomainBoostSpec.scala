package com.socrata.cetera.services

import com.rojoma.json.v3.ast.{JNumber, JString}
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}

import com.socrata.cetera.search._
import com.socrata.cetera.types._
import com.socrata.cetera.util.Params

class DomainBoostSpec extends FunSuiteLike with Matchers with TestESData with BeforeAndAfterAll {
  val boostedDatatype = TypeCharts
  val datatypeBoosts =  Map[Datatype, Float](boostedDatatype -> 10F)

  val client: ElasticSearchClient = new TestESClient(testSuiteName)
  val documentClient: DocumentClient = new DocumentClient(client, datatypeBoosts, None, None, Set.empty)
  val domainClient: DomainClient = new DomainClient(client)
  val service: SearchService = new SearchService(documentClient, domainClient)

  override protected def beforeAll(): Unit = {
    bootstrapData()
  }

  override protected def afterAll(): Unit = {
    removeBootstrapData()
    client.close()
  }

  // blue.org and annabelle.island.net get filtered
  val domain_cnames = Seq("petercetera.net", "opendata-demo.socrata.com")

  test("domain boost of 2 should produce top result") {
    domain_cnames.foreach { domain =>
      val (results, _) = service.doSearch(Map(s"""boostDomains[${domain}]""" -> "2").mapValues(Seq(_)))
      results.results.head.metadata("domain") should be(JString(domain))
    }
  }

  test("domain boost of 0.5 should not produce top result") {
    domain_cnames.foreach { domain =>
      val (results, _) = service.doSearch(Map(s"""boostDomains[${domain}]""" -> "0.5").mapValues(Seq(_)))
      results.results.head.metadata("domain") shouldNot be(JString(domain))
    }
  }

  test("domain boosts are not mistaken for custom metadata") {
    domain_cnames.foreach { domain =>
      val (results, _) = service.doSearch(
        Map(s"""boostDomains[${domain}]""" -> "2.34",
            "boostDomains[]" -> "3.45", // if I were custom metadata, I would not match any documents
            Params.context -> domain).mapValues(Seq(_))
      )
      results.results.size should be > 0
    }
  }

  // just documenting behavior; this may not be ideal behavior
  test("boostDomains with no [] is treated as custom metadata") {
    domain_cnames.foreach { domain =>
      val (results, _) = service.doSearch(
        Map(s"""boostDomains[${domain}]""" -> "2.34",
            "boostDomains" -> "3.45", // interpreted as custom metadata field which doesn't match any documents
            Params.context -> domain).mapValues(Seq(_))
      )
      results.results.size should be(0)
    }
  }

  test("datatype boost - increases score when datatype matches") {
    val (results, _) = service.doSearch(Map(
      Params.querySimple -> "best",
      Params.showScore -> "true"
    ).mapValues(Seq(_)))
    val oneBoosted = results.results.find(_.resource.dyn.`type`.! == JString(boostedDatatype.singular)).head
    val oneOtherThing = results.results.find(_.resource.dyn.`type`.! != JString(boostedDatatype.singular)).head

    val datalensScore: Float = oneBoosted.metadata("score") match {
      case n: JNumber => n.toFloat
      case _ => fail()
    }
    val otherScore: Float = oneOtherThing.metadata("score") match {
      case n: JNumber => n.toFloat
      case _ => fail()
    }

    datalensScore should be > otherScore
  }
}
