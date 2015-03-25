package com.socrata.cetera.services

import com.rojoma.json.v3.ast.{JArray, JString}
import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.JsonReader
import org.elasticsearch.action.search._
import org.elasticsearch.common.bytes.BytesArray
import org.elasticsearch.common.text.StringText
import org.elasticsearch.search.internal._
import org.elasticsearch.search.{SearchHitField, SearchShardTarget}
import org.scalatest.{ShouldMatchers, WordSpec}

class SearchServiceSpec extends WordSpec with ShouldMatchers {
  val service = new SearchService(null)

  val searchResponse = {
    val shardTarget = new SearchShardTarget("1", "datasets", 1)
    val score = 0.12345f

    val resource = "\"resource\":{\"I'm\":\"OK\",\"you're\":\"so-so\"}"

    val dataset_socrata_id =
      "\"socrata_id\":{\"domain_cname\":[\"socrata.com\"],\"dataset_id\":\"four-four\"}"
    val page_socrata_id =
      "\"socrata_id\":{\"domain_cname\":[\"first-socrata.com\", \"second-socrata.com\"],\"dataset_id\":\"four-four\",\"page_id\":\"fore-fore\"}"

    val dataset_source = new BytesArray("{" + List(resource, dataset_socrata_id).mkString(",") + "}")
    val page_source = new BytesArray("{" + List(resource, page_socrata_id).mkString(",") + "}")

    val datasetHit = new InternalSearchHit(1, "46_3yu6-fka7", new StringText("dataset"), null)
    datasetHit.shardTarget(shardTarget)
    datasetHit.sourceRef(dataset_source)
    datasetHit.score(score)

    val pageHit = new InternalSearchHit(1, "64_6uy3-7akf", new StringText("page"), null)
    pageHit.shardTarget(shardTarget)
    pageHit.sourceRef(page_source)
    pageHit.score(score)

    val hits = Array[InternalSearchHit](datasetHit, pageHit)
    val internalSearchHits = new InternalSearchHits(hits, 3037, 1.0f)
    val internalSearchResponse = new InternalSearchResponse(internalSearchHits, null, null, null, false, false)

    new SearchResponse(internalSearchResponse, null, 15, 15, 4, Array[ShardSearchFailure]())
  }

  "SearchService" should {
    "extract and format resources from SearchResponse" in {
      val resource = j"""{ "I'm" : "OK", "you're" : "so-so" }"""

      val searchResults = service.format(searchResponse)

      searchResults.resultSetSize should be (None) // not yet added
      searchResults.timings should be (None) // not yet added

      val results = searchResults.results
      results should be ('nonEmpty)
      results.size should be (2)

      val datasetResponse = results(0)
      datasetResponse.resource should be (j"""${resource}""")
      datasetResponse.classification should be (Classification(JArray(Seq()), JArray(Seq())))

      datasetResponse.metadata.get("domain") match {
        case Some(domain) => domain should be (JString("socrata.com"))
        case None => fail("metadata.domain field missing")
      }

      datasetResponse.link should be (JString("https://socrata.com/ux/dataset/four-four"))

      val pageResponse = results(1)
      pageResponse.resource should be (j"""${resource}""")
      pageResponse.classification should be (Classification(JArray(Seq()), JArray(Seq())))

      pageResponse.metadata.get("domain") match {
        case Some(domain) => domain should be (JString("second-socrata.com"))
        case None => fail("metadata.domain field missing")
      }

      pageResponse.link should be (JString("https://second-socrata.com/view/fore-fore"))
    }
  }
}
