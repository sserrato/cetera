package com.socrata.cetera.services

import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.JsonReader
import org.scalatest.{ShouldMatchers, WordSpec}

class SearchServiceSpec extends WordSpec with ShouldMatchers {
  val service = new SearchService(null)

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
}
