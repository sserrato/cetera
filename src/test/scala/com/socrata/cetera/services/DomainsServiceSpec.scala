package com.socrata.cetera.services

import com.rojoma.json.v3.ast.JNumber.JUncheckedStringNumber
import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.JsonReader
import org.scalatest.{ShouldMatchers, WordSpec}

class DomainsServiceSpec extends WordSpec with ShouldMatchers {
  val service = new DomainsService(null)

  "DomainsService" should {
    val es_response = j"""{
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
        "domain_resources_count" : {
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

    "extract" in {
      val expected = Stream(
        j"""{ "key" : "onethousand.example.com", "doc_count" : 1000 }""",
        j"""{ "key" : "two-thirty-four.example.com", "doc_count" : 234 }""",
        j"""{ "key" : "seven-ate-nine.com", "doc_count" : 78 }""",
        j"""{ "key" : "poor-bono.example.com", "doc_count" : 1 }"""
      )

      val actual = service.extract(es_response)

      (actual, expected).zipped.foreach{ (a, e) => a should be(e) }
    }

    "format" in {
      val expected = Stream(
        j"""{ "domain" : "onethousand.example.com", "count" : 1000}""",
        j"""{ "domain" : "two-thirty-four.example.com", "count" : 234}""",
        j"""{ "domain" : "seven-ate-nine.com", "count" : 78}""",
        j"""{ "domain" : "poor-bono.example.com", "count" : 1}"""
      ).map{ jo => jo.toMap } // sorry, type systems

      val extracted = service.extract(es_response)
      val formatted = service.format(extracted)

      for {
        results <- formatted.get("results")
        domain_counts <- results.get("domain_counts")
      } {
        (domain_counts, expected).zipped.foreach{ (a, e) => a should be (e) }
      }
    }

    // Well, no, it shouldn't actually.
    "fail silently when the expected path to resources does not exist" in {
      val body = j"""{}"""
      val aggregations = service.extract(body)

      aggregations.size should be (0)
    }
  }
}
