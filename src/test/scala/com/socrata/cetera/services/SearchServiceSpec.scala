package com.socrata.cetera.services

import com.rojoma.json.v3.ast.{JString, JValue}
import com.rojoma.json.v3.interpolation._
import com.socrata.cetera._
import com.socrata.cetera.search.{ElasticSearchClient, TestESClient, TestESData}
import com.socrata.cetera.types._
import org.elasticsearch.action.search._
import org.elasticsearch.common.bytes.BytesArray
import org.elasticsearch.common.text.StringText
import org.elasticsearch.search.aggregations.{InternalAggregation, InternalAggregations}
import org.elasticsearch.search.facet.{Facet, InternalFacets}
import org.elasticsearch.search.internal._
import org.elasticsearch.search.suggest.Suggest
import org.elasticsearch.search.{SearchHitField, SearchShardTarget}
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}

import scala.collection.JavaConverters._

class SearchServiceSpec extends FunSuiteLike with Matchers {
  val service = new SearchService(None)

  val emptySearchHitMap = Map[String,SearchHitField]().asJava

  val searchResponse = {
    val shardTarget = new SearchShardTarget("1", IndexCatalog, 1)
    val score = 0.12345f

    val resource = "\"resource\":{\"name\": \"Just A Test\", \"I'm\":\"OK\",\"you're\":\"so-so\"}"

    val datasetSocrataId =
      "\"socrata_id\":{\"domain_cname\":[\"socrata.com\"],\"dataset_id\":\"four-four\"}"
    val pageSocrataId =
      "\"socrata_id\":{\"domain_cname\":[\"first-socrata.com\", \"second-socrata.com\"],\"dataset_id\":\"four-four\",\"page_id\":\"fore-fore\"}"

    val datasetDatatype = "\"datatype\":\"dataset\""
    val datasetViewtype = "\"viewtype\":\"\""
    val pageDatatype = "\"datatype\":\"datalens\""
    val pageViewtype = "\"viewtype\":\"\""

    val datasetSource = new BytesArray("{" + List(resource, datasetDatatype, datasetViewtype, datasetSocrataId).mkString(",") + "}")
    val pageSource = new BytesArray("{" + List(resource, pageDatatype, pageViewtype, pageSocrataId).mkString(",") + "}")

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

  test("extract and format resources from SearchResponse") {
    val resource = j"""{ "name" : "Just A Test", "I'm" : "OK", "you're" : "so-so" }"""

    val searchResults = service.format(showScore = false, searchResponse)

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

    val pageResponse = results(1)
    pageResponse.resource should be (j"""${resource}""")
    pageResponse.classification should be (Classification(Seq.empty[JValue], Seq.empty[JValue], None, None, None))

    pageResponse.metadata.get("domain") match {
      case Some(domain) => domain should be (JString("second-socrata.com"))
      case None => fail("metadata.domain field missing")
    }
  }

  test("build base urls and pretty seo urls") {
    val cname = "tempuri.org"
    val category = Some("Public Safety")
    val name = "Seattle Police Department 911 Incident Response"
    val id = "1234-abcd"

    val dt = "datatype"
    val vt = "viewtype"

    val xp = "expectedPermalink"
    val xpDefault = "/d/"

    val xs = "expectedSeolink"
    val xsDefault = "/Public-Safety/Seattle-Police-Department-911-Incident-Response/"

    Seq(
      Map(dt -> "calendar"),
      Map(dt -> "chart"),
      Map(dt -> "datalens", xp -> "/view/"),
      Map(dt -> "chart", vt -> "datalens", xp -> "/view/"),
      Map(dt -> "map", vt -> "datalens", xp -> "/view/"),
      Map(dt -> "dataset"),
      Map(dt -> "file"),
      Map(dt -> "filter"),
      Map(dt -> "form"),
      Map(dt -> "map", vt -> "geo"),
      Map(dt -> "map", vt -> "tabular"),
      Map(dt -> "href"),
      Map(dt -> "story", xp -> "/stories/s/", xs -> "/stories/s/")
    ).foreach { t =>
      val urls = SearchService.links(cname, DatatypeSimple(t.get(dt)), t.get(vt), id, category, name)
      urls.getOrElse("permalink", fail()).string should include(t.getOrElse(xp, xpDefault))
      urls.getOrElse("link", fail()).string should include(t.getOrElse(xs, xsDefault))
    }
  }

  test("pretty seo url - missing/blank category defaults to 'dataset'") {
    val cname = "tempuri.org"
    val id = "1234-asdf"
    val name = "this is a name"

    Seq(None, Some("")).foreach { category =>
      val urls = SearchService.links(cname, Option(TypeDatasets), None, id, category, name)
      urls.getOrElse("link", fail()).string should include("/dataset/this-is-a-name/1234-asdf")
    }
  }

  test("pretty seo url - missing/blank name defaults to '-'") {
    val cname = "tempuri.org"
    val id = "1234-asdf"
    val category = Some("this-is-a-category")

    Seq(null, "").foreach { name =>
      val urls = SearchService.links(cname, Option(TypeDatasets), None, id, category, name)
      urls.getOrElse("link", fail()).string should include("/this-is-a-category/-/1234-asdf")
    }
  }

  test("pretty seo url - limit 50 characters") {
    val cname = "tempuri.org"
    val id = "1234-asdf"
    val category = Some("A super long category name is not very likely but we will protect against it anyway")
    val name = "More commonly customers may write a title that is excessively verbose and it will hit this limit"
    val urls = SearchService.links(cname, Option(TypeDatasets), None, id, category, name)
    urls.getOrElse("link", fail()).string should include("/A-super-long-category-name-is-not-very-likely-but-/More-commonly-customers-may-write-a-title-that-is-/1234-asdf")
  }

  // NOTE: depending on your editor rendering, these RTL strings might look AWESOME(ly different)!
  // scalastyle:off non.ascii.character.disallowed
  test("pretty seo url - allows non-english unicode") {
    val cname = "tempuri.org"
    val id = "1234-asdf"
    val category = Some("بيانات عن الجدات")
    val name = "愛"
    val urls = SearchService.links(cname, Option(TypeDatasets), None, id, category, name)
    urls.getOrElse("link", fail()).string should include("بيانات-عن-الجدات")
    urls.getOrElse("link", fail()).string should include("愛")
  }

  ignore("es client - min should match") {}
  ignore("es client - slop") {}
  ignore("es client - function score") {}
  ignore("es client - advanced query") {}
  ignore("es client - score boosts") {}
  ignore("es client - domain metadata filter") {}
  ignore("es client - script score functions") {}
  ignore("es client - query with no filters, maybe?") {}
  ignore("es client - sort field asc/desc") {}
  ignore("popularity") {}
  ignore("update frequency") {}
  ignore("domain cname unexpected json value") {}
  ignore("query parameter parser - errors") {}
}

class SearchServiceSpecWithTestData extends FunSuiteLike with Matchers with TestESData with BeforeAndAfterAll {
  val client: ElasticSearchClient = new TestESClient(testSuiteName)
  val service: SearchService = new SearchService(Some(client))

  override protected def beforeAll(): Unit = {
    bootstrapData()
  }

  override protected def afterAll(): Unit = {
    removeBootstrapData()
    client.close()
  }

  test("search response contains pretty and perma links") {
    service.doSearch(Map.empty)._1.results.foreach { r =>
      val dsid = r.resource.dyn.id.!.asInstanceOf[JString].string

      val perma = "(d|stories/s|view)"
      val alphanum = "[\\p{L}\\p{N}]+" // all of the test data have proper categories and names
      val pretty = s"$alphanum/$alphanum"

      r.permalink.string should endWith regex s"/$perma/$dsid"
      r.link.string should endWith regex s"/$pretty/$dsid"
    }
  }
}
