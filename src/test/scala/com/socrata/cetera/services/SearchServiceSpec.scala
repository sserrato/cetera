package com.socrata.cetera.services

import com.rojoma.json.v3.io.JsonReader
import com.rojoma.json.v3.interpolation._
import org.elasticsearch.node.NodeBuilder.nodeBuilder
import org.scalatest.{ShouldMatchers, WordSpec}

class SearchServiceSpec extends WordSpec with ShouldMatchers {
  val node = nodeBuilder().local(true).node()
  val client = node.client()
  val service = new SearchService(client)

  "SearchService" should {
    "extract resources from parsed SearchResponse" in {
      val body = j"""{
        "_shards": {
          "failed": 0,
          "successful": 15,
          "total": 15
        },
        "hits": {
          "hits": [
          {
            "_source": {
              "resource": {
                "I": "super",
                "You": "okay too"
              }
            }
          },
          {
            "_source": {
              "resource": {
                "He": "Me",
                "See": "We"
              }
            }
          }
          ],
          "max_score": 1.0,
          "total": 3037
        },
        "timed_out": false,
        "took": 4
      }"""

      val resources = service.extractResources(body)

      resources.size should be (2)
    }

    // Well, no, it shouldn't actually.
    "fail silently when the expected path to resources does not exist" in {
      val body = j"""{}"""
      val resources = service.extractResources(body)

      resources.size should be (0)
    }
  }

  // These are brittle tests that will break as search features are added
  "Request builder" should {
    "construct a default catalog query correctly" in {
      val expected = j"""{
        "from" : 0,
        "size" : 100,
        "query" : {
          "filtered" : {
            "query" : {
              "match_all" : { }
            },
            "filter" : {
              "match_all" : { }
            }
          }
        }
      }"""

      val request = service.buildSearchRequest(
        searchQuery = None,
        domains = None,
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
        "from" : 10,
        "size" : 20,
        "query" : {
          "filtered" : {
            "query" : {
              "match" : {
                "_all" : {
                  "query" : "search query terms",
                  "type" : "boolean"
                }
              }
            },
            "filter" : {
              "or" : {
                "filters" : [ {
                  "term" : {
                    "domain_cname_exact" : "www.example.com"
                  }
                  }, {
                    "term" : {
                      "domain_cname_exact" : "test.example.com"
                    }
                    }, {
                      "term" : {
                        "domain_cname_exact" : "socrata.com"
                      }
                    } ]
              }
            }
          }
        }
      }"""

      val request = service.buildSearchRequest(
        searchQuery = Some("search query terms"),
        domains = Some("www.example.com,test.example.com,socrata.com"),
        only = Some("dataset"), // this doesn't end up in the json query string
        10,
        20
      )
      val actual = JsonReader.fromString(request.toString)

      actual should be (expected)
      request.request.types should be (Array[String]("dataset"))
    }
  }

  node.close()
}
