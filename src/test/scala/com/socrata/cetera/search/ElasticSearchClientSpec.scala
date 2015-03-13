package com.socrata.cetera.search

import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.JsonReader
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

class ElasticSearchClientSpec extends WordSpec with ShouldMatchers {
  val client = new LocalESClient()  // Remember to close() me!!

  // These are brittle tests that will break as search features are added
  // The brittleness is deliberate. The query building is not finalized.
  "Request builder" should {
    "construct a default catalog query correctly" in {
      val expected = j"""{
        "from" : 0,
        "size" : 100,
        "query" : {
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
        }
      }"""

      val request = client.buildSearchRequest(
        searchQuery = None,
        domains = None,
        categories = None,
        tags = None,
        only = None, // type restriction not in the json query string
        offset = 0,
        limit = 100
      )
      val actual = JsonReader.fromString(request.toString)

      actual should be (expected)
      request.request.types should be (Array[String]())
    }

    "construct a filtered match query correctly" in {
      val expected = j"""{
        "from": 10,
        "size": 20,
        "query": {
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
        }
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

  client.close() // Important!!
}
