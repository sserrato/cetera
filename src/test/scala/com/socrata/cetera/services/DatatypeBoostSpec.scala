package com.socrata.cetera.services

import com.rojoma.json.v3.ast.{JNumber, JString}
import com.socrata.cetera.search.{ElasticSearchClient, TestESClient, TestESData}
import com.socrata.cetera.types._
import com.socrata.cetera.util.Params
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}

class DatatypeBoostSpec extends FunSuiteLike with Matchers with TestESData with BeforeAndAfterAll {
  val boostedDatatype = TypeDatasets
  val client: ElasticSearchClient = new TestESClient(testSuiteName, Map(boostedDatatype -> 10F))
  val service: SearchService = new SearchService(Some(client))

  override protected def beforeAll(): Unit = {
    bootstrapData()
  }

  override protected def afterAll(): Unit = {
    removeBootstrapData()
    client.close()
  }

  test("datatype boost - increases score when datatype matches") {
    val (results, _) = service.doSearch(Map(
      Params.querySimple -> "fourth",
      Params.showScore -> "true"
    ))
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
