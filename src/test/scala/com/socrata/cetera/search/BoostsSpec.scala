package com.socrata.cetera.search

import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.JsonReader
import org.scalatest.{ShouldMatchers, WordSpec}
import org.elasticsearch.index.query.{BoolQueryBuilder, QueryBuilders}

import com.socrata.cetera.types.{Datatype, TypeDatalenses, TypeDatasets, TypeFilters}

class BoostsSpec extends WordSpec with ShouldMatchers {
  "boostDatatypes" should {
    "add datatype boosts to the query" in {
      val query = QueryBuilders.boolQuery()
      val datatypeBoosts = Map[Datatype, Float](
        TypeDatasets -> 1.23f,
        TypeDatalenses -> 2.34f,
        TypeFilters -> 0.98f
      )

      val expectedJson = j"""{
        "bool" : {
          "should" : [
            { "term" : { "datatype" : { "value" : "dataset", "boost" : 1.23 } } },
            { "term" : { "datatype" : { "value" : "datalens", "boost" : 2.34 } } },
            { "term" : { "datatype" : { "value" : "filter", "boost" : 0.98 } } }
          ]
        }
      }"""

      val actual = Boosts.boostDatatypes(query, datatypeBoosts)
      val actualJson = JsonReader.fromString(actual.toString)

      actualJson should be (expectedJson)
    }

    "return empty bool if called with empty map" in {
      val query = QueryBuilders.boolQuery()
      val datatypeBoosts = Map.empty[Datatype, Float]
      val expectedJson = j"""{"bool" : { } }"""
      val actual = Boosts.boostDatatypes(query, datatypeBoosts)
      val actualJson = JsonReader.fromString(actual.toString)
      actualJson should be(expectedJson)
    }
  }

  "boostDomains" should {
    "add domain cname boosts to the query" in {
      val query = QueryBuilders.boolQuery()
      val domainBoosts = Map[String, Float](
        "example.com" -> 1.23f,
        "data.seattle.gov" -> 4.56f
        )

      val expectedJson = j"""{
        "bool" : {
          "should" : [
            { "term" : { "socrata_id.domain_cname.raw" : { "value" : "example.com", "boost" : 1.23 } } },
            { "term" : { "socrata_id.domain_cname.raw" : { "value" : "data.seattle.gov", "boost" : 4.56 } } }
          ]
        }
      }"""

      val actual = Boosts.boostDomains(query, domainBoosts)
      val actualJson = JsonReader.fromString(actual.toString)

      actualJson should be (expectedJson)
    }

    "return empty bool if called with empty map" in {
      val query = QueryBuilders.boolQuery()
      val domainBoosts = Map.empty[String, Float]
      val expectedJson = j"""{"bool" : { } }"""

      val actual = Boosts.boostDomains(query, domainBoosts)
      val actualJson = JsonReader.fromString(actual.toString)
      actualJson should be(expectedJson)
    }
  }
}
