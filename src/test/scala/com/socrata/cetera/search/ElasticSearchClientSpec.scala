package com.socrata.cetera.search

import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.JsonReader
import org.elasticsearch.action.search.SearchType.COUNT
import org.elasticsearch.node.NodeBuilder.nodeBuilder
import org.scalatest.{ShouldMatchers, WordSpec}

class LocalESClient() extends ElasticSearchClient("local", 5704, "useless") {
  val node = nodeBuilder().local(true).node()
  override val client = node.client()
  override def close(): Unit = {
    node.close();
    client.close()
  }
}

// These are brittle tests that will break as search features are added
// The brittleness is deliberate. Query building is not finalized.
//
class ElasticSearchClientSpec extends WordSpec with ShouldMatchers {
  val client = new LocalESClient()  // Remember to close() me!!

  val defaultQuery = j"""{
    "filtered" :
    {
      "query" : { "match_all" : {} },
      "filter" :
      {
        "and" : {
          "filters" : [
          { "match_all" : {} },
          { "match_all" : {} },
          { "match_all" : {} }
          ]
        }
      }
    }
  }"""

  val complexQuery = j"""{
    "filtered": {
      "filter": {
        "and": {
          "filters" : [
          {
            "terms" :
            {
              "socrata_id.domain_cname.raw" : [
              "www.example.com",
              "test.example.com",
              "socrata.com"
              ]
            }
          },
          {
            "terms" :
            {
              "animl_annotations.category_names.raw" : [
              "Social Services",
              "Environment",
              "Housing & Development"
              ]
            }
          },
          {
            "terms" :
            {
              "animl_annotations.tag_names.raw" : [
              "taxi",
              "art",
              "clowns"
              ]
            }
          }
          ]
        }
      },
      "query": {
        "match": {
          "_all": {
            "query": "search query terms",
            "type": "boolean"
          }
        }
      }
    }
  }"""

  "buildBaseRequest" should {
    "construct a default match all query" in {
      val expected = j"""{
        "query" : ${defaultQuery}
      }"""

      val request = client.buildBaseRequest(
        searchQuery = None,
        domains = None,
        categories = None,
        tags = None,
        only = None
      )
      val actual = JsonReader.fromString(request.toString)

      actual should be (expected)
      request.request.types should be (Array())
    }
  }

  "buildSearchRequest" should {
    "construct a filtered match query" in {
      val expected = j"""{
        "from": 10,
        "size": 20,
        "query": ${complexQuery}
      }"""

      val request = client.buildSearchRequest(
        searchQuery = Some("search query terms"),
        domains = Some(Set("www.example.com", "test.example.com", "socrata.com")),
        categories = Some(Set("Social Services", "Environment", "Housing & Development")),
        tags = Some(Set("taxi", "art", "clowns")),
        only = Some("dataset"), // this doesn't end up in the json query string
        10,
        20
      )

      val actual = JsonReader.fromString(request.toString)

      actual should be (expected)
      request.request.types should be (Array[String]("dataset"))
    }
  }

  "buildCountRequest" should {
    "construct a default search query with aggregation" in {
      val expected = j"""{
        "query" : ${defaultQuery},
        "aggregations" : {
          "counts" :
          {
            "terms" :
            {
              "field" : "arbitrary.field_name.raw",
              "size" : 0,
              "order" : { "_count" : "desc" }
            }
          }
        }
      }"""

      val request = client.buildCountRequest(
        field = "arbitrary.field_name.raw",
        searchQuery = None,
        domains = None,
        categories = None,
        tags = None,
        only = None
      )

      val actual = JsonReader.fromString(request.toString)

      actual should be (expected)
      request.request.searchType should be (COUNT)
    }

    "construct a filtered match query with aggregation" in {
      val expected = j"""{
        "query" : ${complexQuery},
        "aggregations" : {
          "counts" :
          {
            "terms" :
            {
              "field" : "arbitrary.field_name.raw",
              "size" : 0,
              "order" : { "_count" : "desc" }
            }
          }
        }
      }"""

      val request = client.buildCountRequest(
        "arbitrary.field_name.raw",
        searchQuery = Some("search query terms"),
        domains = Some(Set("www.example.com", "test.example.com", "socrata.com")),
        categories = Some(Set("Social Services", "Environment", "Housing & Development")),
        tags = Some(Set("taxi", "art", "clowns")),
        only = Some("dataset")
      )

      val actual = JsonReader.fromString(request.toString)

      actual should be (expected)
      request.request.searchType should be (COUNT)
    }
  }

  client.close() // Important!!
}
