package com.socrata.cetera.services

import com.rojoma.json.v3.ast.{JString, JValue}
import com.rojoma.json.v3.interpolation._
import com.socrata.cetera._
import org.elasticsearch.action.search._
import org.elasticsearch.common.bytes.BytesArray
import org.elasticsearch.common.text.StringText
import org.elasticsearch.search.aggregations.{InternalAggregation, InternalAggregations}
import org.elasticsearch.search.facet.{Facet, InternalFacets}
import org.elasticsearch.search.internal._
import org.elasticsearch.search.suggest.Suggest
import org.elasticsearch.search.{SearchHitField, SearchShardTarget}
import org.scalatest.{ShouldMatchers, WordSpec}

import scala.collection.JavaConverters._

class SearchServiceSpec extends WordSpec with ShouldMatchers {
  val service = new SearchService(None)

  val emptySearchHitMap = Map[String,SearchHitField]().asJava

  val searchResponse = {
    val shardTarget = new SearchShardTarget("1", IndexDatasets, 1)
    val score = 0.12345f

    val resource = "\"resource\":{\"I'm\":\"OK\",\"you're\":\"so-so\"}"

    val datasetSocrataId =
      "\"socrata_id\":{\"domain_cname\":[\"socrata.com\"],\"dataset_id\":\"four-four\"}"
    val pageSocrataId =
      "\"socrata_id\":{\"domain_cname\":[\"first-socrata.com\", \"second-socrata.com\"],\"dataset_id\":\"four-four\",\"page_id\":\"fore-fore\"}"

    val datasetSource = new BytesArray("{" + List(resource, datasetSocrataId).mkString(",") + "}")
    val pageSource = new BytesArray("{" + List(resource, pageSocrataId).mkString(",") + "}")

    val datasetHit = new InternalSearchHit(1, "46_3yu6-fka7", new StringText("dataset"), emptySearchHitMap)
    datasetHit.shardTarget(shardTarget)
    datasetHit.sourceRef(datasetSource)
    datasetHit.score(score)

    val updateFreq: SearchHitField = new InternalSearchHitField(
      "update_freq", List.empty[Object].asJava)

    val popularity: SearchHitField = new InternalSearchHitField(
      "popularity", List.empty[Object].asJava)

    val pageHit = new InternalSearchHit(1, "64_6uy3-7akf", new StringText("page"), emptySearchHitMap)
    pageHit.shardTarget(shardTarget)
    pageHit.sourceRef(pageSource)
    pageHit.score(score)

    val hits = Array[InternalSearchHit](datasetHit, pageHit)
    val internalSearchHits = new InternalSearchHits(hits, 3037, 1.0f)
    val internalSearchResponse = new InternalSearchResponse(
      internalSearchHits,
      new InternalFacets(List[Facet]().asJava),
      new InternalAggregations(List[InternalAggregation]().asJava),
      new Suggest(),
      false,
      false)

    new SearchResponse(internalSearchResponse, "", 15, 15, 4, Array[ShardSearchFailure]())
  }

  "SearchService" should {
    "extract and format resources from SearchResponse" in {
      val resource = j"""{ "I'm" : "OK", "you're" : "so-so" }"""

      val searchResults = service.format(showFeatureVals = false, showScore = false, searchResponse)

      searchResults.resultSetSize should be (None) // not yet added
      searchResults.timings should be (None) // not yet added

      val results = searchResults.results
      results should be ('nonEmpty)
      results.size should be (2)

      val datasetResponse = results(0)
      datasetResponse.resource should be (j"""${resource}""")
      datasetResponse.classification should be (Classification(Seq.empty[JValue], Seq.empty[JValue], None, None, None))

      datasetResponse.metadata.get("domain") match {
        case Some(domain) => domain should be (JString("socrata.com"))
        case None => fail("metadata.domain field missing")
      }

      datasetResponse.link should be (JString("https://socrata.com/d/four-four"))

      val pageResponse = results(1)
      pageResponse.resource should be (j"""${resource}""")
      pageResponse.classification should be (Classification(Seq.empty[JValue], Seq.empty[JValue], None, None, None))

      pageResponse.metadata.get("domain") match {
        case Some(domain) => domain should be (JString("second-socrata.com"))
        case None => fail("metadata.domain field missing")
      }

      pageResponse.link should be (JString("https://second-socrata.com/view/fore-fore"))
    }
  }
}
