package com.socrata.cetera.search

import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.JsonReader
import org.elasticsearch.index.query.QueryBuilders
import org.scalatest.{ShouldMatchers, WordSpec}

import com.socrata.cetera.types.{Datatype, DatalensDatatype, DatasetDatatype, FilterDatatype}

class BoostsSpec extends WordSpec with ShouldMatchers {
  "applyDatatypeBoosts" should {
    "add datatype boosts to the query" in {
      val query = QueryBuilders.functionScoreQuery(QueryBuilders.matchAllQuery)
      val datatypeBoosts = Map[Datatype, Float](
        DatasetDatatype -> 1.23f,
        DatalensDatatype -> 2.34f,
        FilterDatatype -> 0.98f
      )
      Boosts.applyDatatypeBoosts(query, datatypeBoosts)

      val expectedJson = j"""{
        "function_score": {
          "functions": [
            {
              "filter": { "term": { "datatype" : "dataset" } },
              "weight": 1.23
            },
            {
              "filter": { "term": { "datatype" : "datalens" } },
              "weight": 2.34
            },
            {
              "filter": { "term": { "datatype" : "filter" } },
              "weight": 0.98
            }
          ],
          "query": {
            "match_all": {}
          }
        }
      }"""

      val actualJson = JsonReader.fromString(query.toString)
      actualJson should be (expectedJson)
    }

    "do nothing to the query if given no datatype boosts" in {
      val query = QueryBuilders.functionScoreQuery(QueryBuilders.matchAllQuery)
      val datatypeBoosts = Map.empty[Datatype, Float]

      val beforeJson = JsonReader.fromString(query.toString)
      Boosts.applyDatatypeBoosts(query, datatypeBoosts)
      val afterJson = JsonReader.fromString(query.toString)

      afterJson should be(beforeJson)
    }
  }

  "boostDomains" should {
    "add domain cname boosts to the query" in {
      val query = QueryBuilders.functionScoreQuery(QueryBuilders.matchAllQuery)
      val domainBoosts = Map[Int, Float](
        0 -> 1.23f,
        7 -> 4.56f
      )

      Boosts.applyDomainBoosts(query, domainBoosts)

      val expectedJson = j"""{
        "function_score": {
          "functions": [
            {
              "filter": { "term": { "socrata_id.domain_id": 0 } },
              "weight": 1.23
            },
            {
              "filter": { "term": { "socrata_id.domain_id": 7 } },
              "weight": 4.56
            }
          ],
          "query": {
            "match_all": {}
          }
        }
      }"""

      val actualJson = JsonReader.fromString(query.toString)

      actualJson should be (expectedJson)
    }

    "do nothing to the query if given no domain boosts" in {
      val query = QueryBuilders.functionScoreQuery(QueryBuilders.matchAllQuery)
      val domainBoosts = Map.empty[Int, Float]

      val beforeJson = JsonReader.fromString(query.toString)
      Boosts.applyDomainBoosts(query, domainBoosts)
      val afterJson = JsonReader.fromString(query.toString)

      afterJson should be(beforeJson)
    }
  }
}
