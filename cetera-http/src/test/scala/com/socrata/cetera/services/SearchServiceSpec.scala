package com.socrata.cetera.services

import java.io.{ByteArrayInputStream, File}
import java.nio.charset.{Charset, CodingErrorAction}
import java.util.Collections
import javax.servlet.http.HttpServletRequest
import scala.collection.JavaConverters._

import com.rojoma.json.v3.ast.{JString, JValue}
import com.rojoma.json.v3.interpolation._
import com.rojoma.simplearm.v2.managed
import com.socrata.http.server.HttpRequest
import com.socrata.http.server.HttpRequest.AugmentedHttpServletRequest
import org.apache.http.HttpStatus.SC_INTERNAL_SERVER_ERROR
import org.elasticsearch.action.search._
import org.elasticsearch.common.bytes.BytesArray
import org.elasticsearch.common.text.StringText
import org.elasticsearch.search.aggregations.{InternalAggregation, InternalAggregations}
import org.elasticsearch.search.facet.{Facet, InternalFacets}
import org.elasticsearch.search.internal._
import org.elasticsearch.search.suggest.Suggest
import org.elasticsearch.search.{SearchHitField, SearchShardTarget}
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}
import org.springframework.mock.web.{DelegatingServletInputStream, MockHttpServletResponse}

import com.socrata.cetera._
import com.socrata.cetera.auth.AuthParams
import com.socrata.cetera.errors.DomainNotFoundError
import com.socrata.cetera.handlers.{FormatParamSet, Params}
import com.socrata.cetera.metrics.BalboaClient
import com.socrata.cetera.response.{Classification, Format, SearchResult}
import com.socrata.cetera.search._
import com.socrata.cetera.types._

class SearchServiceSpec extends FunSuiteLike with Matchers with BeforeAndAfterAll {
  val testSuiteName = getClass.getSimpleName.toLowerCase
  val client = new TestESClient(testSuiteName)
  val httpClient = new TestHttpClient()
  val coreClient = new TestCoreClient(httpClient, 8036)
  val domainClient = new DomainClient(client, coreClient, testSuiteName)
  val documentClient = new DocumentClient(client, domainClient, testSuiteName, None, None, Set.empty)
  val balboaClient = new BalboaClient("/tmp/metrics")
  val service = new SearchService(documentClient, domainClient, balboaClient, coreClient)

  override protected def afterAll(): Unit = {
    client.close() // Important!!
    httpClient.close()
  }

  val emptySearchHitMap = Map[String,SearchHitField]().asJava

  val domains = Set(
    Domain(0, "socrata.com", None, None, true, false, false, false, false),
    Domain(1, "first-socrata.com", None, None, true, false, false, false, false),
    Domain(2, "second-socrata.com", None, None, true, false, false, false, false))
  val domainSet = DomainSet(domains = domains)

  val searchResponse = {
    val shardTarget = new SearchShardTarget("1", "catalog", 1)
    val score = 0.12345f

    val resource = "\"resource\":{\"name\": \"Just A Test\", \"I'm\":\"OK\",\"you're\":\"so-so\"}"

    val datasetSocrataId =
      "\"socrata_id\":{\"domain_id\":[0],\"dataset_id\":\"four-four\"}"
    val pageSocrataId =
      "\"socrata_id\":{\"domain_id\":[1,2],\"dataset_id\":\"four-four\",\"page_id\":\"fore-fore\"}"

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

  val badSearchResponse = {
    val shardTarget = new SearchShardTarget("1", "catalog", 1)
    val score = 0.54321f

    val badResource = "\"badResource\":{\"name\": \"Just A Test\", \"I'm\":\"NOT OK\",\"you'll\":\"never know\"}"
    val goodResource = "\"resource\":{\"name\": \"Just A Test\", \"I'm\":\"OK\",\"you're\":\"so-so\"}"

    val datasetSocrataId = "\"socrata_id\":{\"domain_id\":[0],\"dataset_id\":\"four-four\"}"
    val badDatasetSocrataId = "\"socrata_id\":{\"domain_id\":\"i am a string\",\"dataset_id\":\"four-four\"}"
    val pageSocrataId = "\"socrata_id\":{\"domain_id\":[1,2],\"dataset_id\":\"four-four\",\"page_id\":\"fore-fore\"}"

    val datasetDatatype = "\"datatype\":\"dataset\""
    val datasetViewtype = "\"viewtype\":\"\""
    val pageDatatype = "\"datatype\":\"datalens\""
    val pageViewtype = "\"viewtype\":\"\""

    val badResourceDatasetSource = new BytesArray("{" +
      List(badResource, datasetDatatype, datasetViewtype, datasetSocrataId).mkString(",") +
      "}")

    val badSocrataIdDatasetSource = new BytesArray("{" +
      List(goodResource, datasetDatatype, datasetViewtype, badDatasetSocrataId).mkString(",")
      + "}")

    val pageSource = new BytesArray("{" +
      List(goodResource, pageDatatype, pageViewtype, pageSocrataId).mkString(",")
      + "}")

    // A result missing the resource field should get filtered (a bogus doc is often missing expected fields)
    val badResourceDatasetHit = new InternalSearchHit(1, "46_3yu6-fka7", new StringText("dataset"), emptySearchHitMap)
    badResourceDatasetHit.shardTarget(shardTarget)
    badResourceDatasetHit.sourceRef(badResourceDatasetSource)
    badResourceDatasetHit.score(score)

    // A result with corrupted (unparseable) field should also get skipped (instead of raising)
    val badSocrataIdDatasetHit = new InternalSearchHit(1, "46_3yu6-fka7", new StringText("dataset"), emptySearchHitMap)
    badSocrataIdDatasetHit.shardTarget(shardTarget)
    badSocrataIdDatasetHit.sourceRef(badSocrataIdDatasetSource)
    badSocrataIdDatasetHit.score(score)

    val updateFreq: SearchHitField = new InternalSearchHitField("update_freq", List.empty[Object].asJava)
    val popularity: SearchHitField = new InternalSearchHitField("popularity", List.empty[Object].asJava)

    val pageHit = new InternalSearchHit(1, "64_6uy3-7akf", new StringText("page"), emptySearchHitMap)
    pageHit.shardTarget(shardTarget)
    pageHit.sourceRef(pageSource)
    pageHit.score(score)

    val hits = Array[InternalSearchHit](badResourceDatasetHit, badSocrataIdDatasetHit, pageHit)
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
    val domain = Domain(1, "tempuri.org", Some("Title"), Some("Temp Org"), isCustomerDomain = true, moderationEnabled = false, routingApprovalEnabled = false, lockedDown = false, apiLockedDown = false)
    val resource = j"""{ "name" : "Just A Test", "I'm" : "OK", "you're" : "so-so" }"""

    val searchResults = Format.formatDocumentResponse(FormatParamSet(), domainSet, searchResponse)

    searchResults.resultSetSize should be (searchResponse.getHits.getTotalHits)
    searchResults.timings should be (None) // not yet added

    val results = searchResults.results
    results should be ('nonEmpty)
    results.size should be (2)

    val datasetResponse = results(0)
    datasetResponse.resource should be (j"""${resource}""")
    datasetResponse.classification should be (Classification(Seq.empty[JValue], Seq.empty[JValue], None, None, None))

    datasetResponse.metadata.domain should be ("socrata.com")

    val pageResponse = results(1)
    pageResponse.resource should be (j"""${resource}""")
    pageResponse.classification should be (Classification(Seq.empty[JValue], Seq.empty[JValue], None, None, None))

    pageResponse.metadata.domain should be ("second-socrata.com")
  }

  test("SearchResponse does not throw on bad documents it just ignores them") {
    val domain = Domain(1, "tempuri.org", Some("Title"), Some("Temp Org"), isCustomerDomain = true, moderationEnabled = false, routingApprovalEnabled = false, lockedDown = false, apiLockedDown = false)

    val expectedResource = j"""{ "name" : "Just A Test", "I'm" : "OK", "you're" : "so-so" }"""
    val searchResults = Format.formatDocumentResponse(FormatParamSet(), domainSet, badSearchResponse)

    val results = searchResults.results
    results.size should be (1)

    val pageResponse = results(0)
    pageResponse.resource should be (j"""${expectedResource}""")
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
  val client = new TestESClient(testSuiteName)
  val httpClient = new TestHttpClient()
  val coreClient = new TestCoreClient(httpClient, 8037)
  val domainClient = new DomainClient(client, coreClient, testSuiteName)
  val documentClient = new DocumentClient(client, domainClient, testSuiteName, None, None, Set.empty)
  val balboaDir = new File("balboa_test_trash")
  val balboaClient = new BalboaClient(balboaDir.getName)

  val service = new SearchService(documentClient, domainClient, balboaClient, coreClient)

  def emptyAndRemoveDir(dir: File): Unit = {
    if (dir.isDirectory) {
      dir.listFiles().foreach(f => f.delete())
    }
    dir.delete()
  }

  override protected def beforeAll(): Unit = {
    bootstrapData()
    emptyAndRemoveDir(balboaDir)
  }

  override protected def afterAll(): Unit = {
    removeBootstrapData()
    client.close()
    httpClient.close()
    emptyAndRemoveDir(balboaDir)
  }

  def wasSearchQueryLogged(filename: String, q: String): Boolean = {
    val decoder = Charset.forName("UTF-8").newDecoder()
    decoder.onMalformedInput(CodingErrorAction.IGNORE)
    for {
      s <- managed(scala.io.Source.fromFile(filename)(decoder))
    }{
      val entries = s.getLines.toList.head
      entries.contains(s"datasets-search-$q")
    }
  }

  test("search with a non-existent search_context throws a DomainNotFoundError") {
    val params = Map(
      "domains" -> "opendata-demo.socrata.com",
      "search_context" -> "bad-domain.com"
    ).mapValues(Seq(_))
    intercept[DomainNotFoundError] {
      service.doSearch(params, false, AuthParams(), None, None)
    }
  }

  test("search response contains pretty and perma links") {
    service.doSearch(Map.empty, false, AuthParams(), None, None)._2.results.foreach { r =>
      val dsid = r.resource.dyn.id.!.asInstanceOf[JString].string

      val perma = "(d|stories/s|view)"
      val alphanum = "[\\p{L}\\p{N}\\-]+" // all of the test data have proper categories and names
      val pretty = s"$alphanum/$alphanum"

      r.permalink.string should endWith regex s"/$perma/$dsid"
      r.link.string should endWith regex s"/$pretty/$dsid"
    }
  }

  test("if a domain is given in the searchContext and a simple query is given, the query should be logged") {
    val query = "log this query"
    val params = Map(
      "domains" -> "opendata-demo.socrata.com,petercetera.net",
      "search_context" -> "opendata-demo.socrata.com",
      "q" -> query
    ).mapValues(Seq(_))
    service.doSearch(params, false, AuthParams(), None, None)._2.results
    val metricsFile = balboaDir.listFiles()(0)
    wasSearchQueryLogged(metricsFile.getAbsolutePath, query) should be(true)
  }

  test("if a domain is given in the searchContext and an advance query is given, the query should be logged") {
    val query = "(log this query OR don't) AND (check up on it OR don't)"
    val params = Map(
      "domains" -> "opendata-demo.socrata.com,petercetera.net",
      "search_context" -> "opendata-demo.socrata.com",
      "q_internal" -> query
    ).mapValues(Seq(_))
    service.doSearch(params, false, AuthParams(), None, None)._2.results
    val metricsFile = balboaDir.listFiles()(0)
    wasSearchQueryLogged(metricsFile.getAbsolutePath, query) should be(true)
  }

  test("if there is no searchContext, no query should be logged") {
    val query = "don't log this query"
    val params = Map(
      "domains" -> "opendata-demo.socrata.com,petercetera.net",
      "q" -> query
    ).mapValues(Seq(_))
    service.doSearch(params, false, AuthParams(), None, None)._2.results
    val metricsFile = balboaDir.listFiles()(0)
    wasSearchQueryLogged(metricsFile.getAbsolutePath, query) should be(false)
  }

  test("search response without a searchContext should have the correct set of documents") {
    val customerDomainIds = domains.filter(d => d.isCustomerDomain).map(_.domainId)
    val anonymousCustomerDocs = anonymouslyViewableDocs.filter(d => customerDomainIds.contains(d.socrataId.domainId))
    val expectedFxfs = anonymousCustomerDocs.map(_.socrataId.datasetId)

    // this shows that:
    //   * rejected and pending views don't show up regardless of domain setting
    //   * that the ES type returned includes only documents (i.e. no domains)
    //   * that non-customer domains don't show up
    val (_, res, _, _) = service.doSearch(Map.empty, false, AuthParams(), None, None)
    val actualFxfs = res.results.map(_.resource.dyn.id.!.asInstanceOf[JString].string)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("domainBoosts are respected") {
    val params = Map(
      Params.filterDomains -> "petercetera.net,annabelle.island.net",
      s"${Params.boostDomains}[annabelle.island.net]" -> "0.0",
      Params.showScore -> "true"
    )
    val (_, res, _, _) = service.doSearch(params.mapValues(Seq(_)), false, AuthParams(), None, None)
    val metadata = res.results.map(_.metadata)
    val annabelleRes = metadata.filter(_.domain == "annabelle.island.net")
    annabelleRes.foreach{ r =>
      r.score.get should be(0.0)
    }
  }

  test("private documents should always be hidden") {
    val expectedFxfs = Set.empty
    val (_, res, _, _) = service.doSearch(Map(
      Params.filterDomains -> "petercetera.net",
      Params.context -> "petercetera.net",
      Params.querySimple -> "private"
    ).mapValues(Seq(_)), false, AuthParams(), None, None)
    val actualFxfs = res.results.map(_.resource.dyn.id.!.asInstanceOf[JString].string)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("unpublished documents should always be hidden") {
    val expectedFxfs = Set.empty
    val (_, res, _, _) = service.doSearch(Map(
      Params.filterDomains -> "petercetera.net",
      Params.context -> "petercetera.net",
      Params.querySimple -> "unpublished"
    ).mapValues(Seq(_)), false, AuthParams(), None, None)
    val actualFxfs = res.results.map(_.resource.dyn.id.!.asInstanceOf[JString].string)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("hidden documents should be hidden when auth isn't required") {
    val hiddenDoc = docs(4)
    val (_, res, _, _) = service.doSearch(Map(
      Params.filterId -> hiddenDoc.socrataId.datasetId
    ).mapValues(Seq(_)), false, AuthParams(), None, None)
    val actualFxfs = res.results.map(_.resource.dyn.id.!.asInstanceOf[JString].string)
    // ensure the hidden doc didn't come back
    actualFxfs should be('empty)
    // ensure that's b/c it's hidden and not b/c it is private or unpublished
    hiddenDoc.isPublic should be(true)
    hiddenDoc.isPublished should be(true)
  }

  test("not_moderated data federated to a moderated domain should not be in the response") {
    // the domain params will limit us to fxfs 0,4,8 and 1,5,9
    val params = Map(
      "domains" -> "opendata-demo.socrata.com,petercetera.net",
      "search_context" -> "opendata-demo.socrata.com"
    ).mapValues(Seq(_))
    // of those fxfs, only show: fxf-1 is approved and fxf-8 is a default view
    val expectedFxfs = Set("fxf-1", "fxf-8", "zeta-0007")
    val res = service.doSearch(params, false, AuthParams(), None, None)._2.results
    val actualFxfs = res.map(_.resource.dyn.id.!.asInstanceOf[JString].string)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("if a domain has routing & approval, only parent domain approved datasets should show up") {
    // the domain params will limit us to fxfs 2,6,10
    val params = Map(
      "domains" -> "blue.org"
    ).mapValues(Seq(_))
    // only these fxfs are approved by parent domain:
    val expectedFxfs = Set("fxf-10")
    val res = service.doSearch(params, false, AuthParams(), None, None)._2.results
    val actualFxfs = res.map(_.resource.dyn.id.!.asInstanceOf[JString].string)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  // contrast this test to that in SearchServiceSpecWithPrivateData
  test("if a domain is locked and no auth is provided, nothing should come back") {
    val lockedDomain = domains(8).domainCname
    val params = Map(
      "domains" -> lockedDomain,
      "search_context" -> lockedDomain
    ).mapValues(Seq(_))
    val res = service.doSearch(params, false, AuthParams(), None, None)._2.results
    val actualFxfs = res.map(_.resource.dyn.id.!.asInstanceOf[JString].string)
    // ensure nothing comes back
    actualFxfs should be('empty)

    // ensure something could have come back
    val docFrom8 = docs(18)
    docFrom8.socrataId.domainId should be(domains(8).domainId)
    docFrom8.isPublic should be(true)
    docFrom8.isPublished should be(true)
    docFrom8.isApprovedByParentDomain should be(true)
    docFrom8.isModerationApproved.get should be(true)
  }


  test("if a search context has routing & approval, only datasets approved by that domain too should show up") {
    val params = Map(
      "domains" -> "blue.org,annabelle.island.net",
      "search_context" -> "blue.org"
    ).mapValues(Seq(_))
    // only these fxfs are approved by search context AND parent domain:
    val expectedFxfs = Set("fxf-10", "zeta-0002")
    val res = service.doSearch(params, false, AuthParams(), None, None)._2.results
    val actualFxfs = res.map(_.resource.dyn.id.!.asInstanceOf[JString].string)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("if a user is provided, only datasets owned by that user should show up") {
    val params = Map(
      "for_user" -> "robin-hood"
    ).mapValues(Seq(_))
    val expectedFxfs = Set("fxf-0", "fxf-8", "fxf-10", "zeta-0001")
    val res = service.doSearch(params, false, AuthParams(), None, None)._2.results

    val actualFxfs = res.map(_.resource.dyn.id.!.asInstanceOf[JString].string)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  // TODO: consider searching for datasets by user screen name; using new custom analyzer
  ignore("if a user's name is queried, datasets with a matching owner:screen_name should show up") {
    val params = Map(
      "q" -> "John"
    ).mapValues(Seq(_))

    val expectedFxfs = Set("zeta-0002", "zeta-0005")
    val res = service.doSearch(params, false, AuthParams(), None, None)._2.results

    val actualFxfs = res.map(_.resource.dyn.id.!.asInstanceOf[JString].string)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("if a parent dataset is provided, response should only include views derived from that dataset") {
    val params = Map(
      Params.filterParentDatasetId -> "fxf-0"
    ).mapValues(Seq(_))

    val expectedFxfs = Set("fxf-8", "fxf-10")
    val res = service.doSearch(params, false, AuthParams(), None, None)._2.results

    val actualFxfs = res.map(_.resource.dyn.id.!.asInstanceOf[JString].string)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("categories filter should be case insensitive") {
    val paramsTitleCase = Map(
      "domains" -> Seq("petercetera.net"),
      "categories" -> Seq("Personal")
    )
    val paramsLowerCase = paramsTitleCase.mapValues(_.map(_.toLowerCase))
    val paramsUpperCase = paramsTitleCase.mapValues(_.map(_.toUpperCase))

    val (_, resultsTitleCase, _, _) = service.doSearch(paramsTitleCase, false, AuthParams(), None, None)
    val (_, resultsLowerCase, _, _) = service.doSearch(paramsLowerCase, false, AuthParams(), None, None)
    val (_, resultsUpperCase, _, _) = service.doSearch(paramsUpperCase, false, AuthParams(), None, None)

    resultsTitleCase.results should contain theSameElementsAs resultsLowerCase.results
    resultsTitleCase.results should contain theSameElementsAs resultsUpperCase.results
  }

  test("custom domain categories filter should be case insensitive") {
    val paramsTitleCase = Map(
      "domains" -> Seq("petercetera.net"),
      "search_context" -> Seq("petercetera.net"),
      "categories" -> Seq("Alpha")
    )
    val paramsLowerCase = paramsTitleCase.mapValues(_.map(_.toLowerCase))
    val paramsUpperCase = paramsTitleCase.mapValues(_.map(_.toUpperCase))

    val (_, resultsTitleCase, _, _) = service.doSearch(paramsTitleCase, false, AuthParams(), None, None)
    val (_, resultsLowerCase, _, _) = service.doSearch(paramsLowerCase, false, AuthParams(), None, None)
    val (_, resultsUpperCase, _, _) = service.doSearch(paramsUpperCase, false, AuthParams(), None, None)

    resultsTitleCase.results should contain theSameElementsAs resultsLowerCase.results
    resultsTitleCase.results should contain theSameElementsAs resultsUpperCase.results
  }

  test("tags filter should be case insensitive") {
    val paramsTitleCase = Map(
      "domains" -> Seq("petercetera.net"),
      "tags" -> Seq("Happy")
    )
    val paramsLowerCase = paramsTitleCase.mapValues(_.map(_.toLowerCase))
    val paramsUpperCase = paramsTitleCase.mapValues(_.map(_.toUpperCase))

    val (_, resultsTitleCase, _, _) = service.doSearch(paramsTitleCase, false, AuthParams(), None, None)
    val (_, resultsLowerCase, _, _) = service.doSearch(paramsLowerCase, false, AuthParams(), None, None)
    val (_, resultsUpperCase, _, _) = service.doSearch(paramsUpperCase, false, AuthParams(), None, None)

    resultsTitleCase.results should contain theSameElementsAs resultsLowerCase.results
    resultsTitleCase.results should contain theSameElementsAs resultsUpperCase.results
  }

  test("custom domain tags filter should be case insensitive") {
    val paramsTitleCase = Map(
      "domains" -> Seq("petercetera.net"),
      "search_context" -> Seq("petercetera.net"),
      "tags" -> Seq("1-One")
    )
    val paramsLowerCase = paramsTitleCase.mapValues(_.map(_.toLowerCase))
    val paramsUpperCase = paramsTitleCase.mapValues(_.map(_.toUpperCase))

    val (_, resultsTitleCase, _, _) = service.doSearch(paramsTitleCase, false, AuthParams(), None, None)
    val (_, resultsLowerCase, _, _) = service.doSearch(paramsLowerCase, false, AuthParams(), None, None)
    val (_, resultsUpperCase, _, _) = service.doSearch(paramsUpperCase, false, AuthParams(), None, None)

    resultsTitleCase.results should contain theSameElementsAs resultsLowerCase.results
    resultsTitleCase.results should contain theSameElementsAs resultsUpperCase.results
  }

  test("searching for a category should include partial phrase matches") {
    val params = Map(
      "search_context" -> "petercetera.net",
      "domains" -> "petercetera.net",
      "q" -> "Alpha"
    ).mapValues(Seq(_))

    val expectedFxfs = Set("fxf-0", "fxf-8")
    val res = service.doSearch(params, false, AuthParams(), None, None)._2.results

    val actualFxfs = res.map(_.resource.dyn.id.!.asInstanceOf[JString].string)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  // TODO: double check that partial phrase match on a category/tag filter is appropriate
  test("filtering by a category should include partial phrase matches") {
    val params = Map(
      "search_context" -> "petercetera.net",
      "domains" -> "petercetera.net",
      "categories" -> "Alpha"
    ).mapValues(Seq(_))

    val expectedFxfs = Set("fxf-0", "fxf-8")
    val res = service.doSearch(params, false, AuthParams(), None, None)._2.results

    val actualFxfs = res.map(_.resource.dyn.id.!.asInstanceOf[JString].string)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("filtering by a category should NOT include individual term matches") {
    val params = Map(
      "search_context" -> "petercetera.net",
      "domains" -> "petercetera.net",
      // the full category is "Alpha to Omega" and we used to include matches on any one of the terms e.g. Alpha
      "categories" -> "Alpha Beta Gaga"
    ).mapValues(Seq(_))

    val expectedFxfs = Set.empty
    val res = service.doSearch(params, false, AuthParams(), None, None)._2.results

    val actualFxfs = res.map(_.resource.dyn.id.!.asInstanceOf[JString].string)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("sorting by name works") {
    val params = Map("order" -> Seq("name"))
    val (_, results, _, _) = service.doSearch(params, false, AuthParams(), None, None)
    val expected = results.results.map(_.resource.dyn("name").!.asInstanceOf[JString].string).sorted.head
    val firstResult = results.results.head.resource.dyn("name").? match {
      case Right(n) => n should be (JString(expected))
      case Left(_) => fail("resource had no name!")
    }
  }

  test("sorting by name DESC works") {
    val params = Map("order" -> Seq("name DESC"))
    val (_, results, _, _) = service.doSearch(params, false, AuthParams(), None, None)
    val expected = results.results.map(_.resource.dyn("name").!.asInstanceOf[JString].string).sorted.last
    val firstResult = results.results.head.resource.dyn("name").? match {
      case Right(n) => n should be (JString(expected))
      case Left(_) => fail("resource had no name!")
    }
  }

  test("filtering by attribution works") {
    val params = Map("attribution" -> Seq("The Merry Men"))
    val (_, results, _, _) = service.doSearch(params, false, AuthParams(), None, None)
    val expectedFxfs = Set("zeta-0007")
    val actualFxfs = results.results.map(_.resource.dyn.id.!.asInstanceOf[JString].string)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("filtering by attribution is case sensitive") {
    val params = Map("attribution" -> Seq("the merry men"))
    val (_, results, _, _) = service.doSearch(params, false, AuthParams(), None, None)
    val expectedFxfs = Set.empty
    val actualFxfs = results.results.map(_.resource.dyn.id.!.asInstanceOf[JString].string)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("searching for attribution via keyword searches should include individual term matches regardless of case") {
    val params = Map("q" -> Seq("merry men"))
    val (_, results, _, _) = service.doSearch(params, false, AuthParams(), None, None)
    val expectedFxfs = Set("zeta-0007")
    val actualFxfs = results.results.map(_.resource.dyn.id.!.asInstanceOf[JString].string)
    actualFxfs should contain theSameElementsAs expectedFxfs
  }

  test("attribution is included in the resulting resource") {
    val params = Map("q" -> Seq("merry men"))
    val (_, results, _, _) = service.doSearch(params, false, AuthParams(), None, None)
    results.results.headOption.map { case SearchResult(resource, _, _, _, _, _) =>
      resource.dyn.attribution.!.asInstanceOf[JString].string
    } should be(Some("The Merry Men"))
  }

  test("passing a datatype boost should have no effect on the size of the result set") {
    val (_, results, _, _) = service.doSearch(Map.empty, false, AuthParams(), None, None)
    val params = Map("boostFiles" -> Seq("2.0"))
    val (_, resultsBoosted, _, _) = service.doSearch(params, false, AuthParams(), None, None)
    resultsBoosted.resultSetSize should be (results.resultSetSize)
  }

  test("giving a datatype a boost of >1 should promote assets of that type to the top") {
    val params = Map("boostStories" -> Seq("10.0"))
    val (_, results, _, _) = service.doSearch(params, false, AuthParams(), None, None)
    val resultTypes = results.results.map(_.resource.dyn.`type`.!.asInstanceOf[JString].string)
    val topResultType = resultTypes.headOption
    topResultType should be (Some("story"))
  }

  test("giving a datatype a boost of <<1 should demote assets of that type to the bottom") {
    val params = Map("boostStories" -> Seq(".0000001"))
    val (_, results, _, _) = service.doSearch(params, false, AuthParams(), None, None)
    val resultTypes = results.results.map(_.resource.dyn.`type`.!.asInstanceOf[JString].string)
    val lastResultType = resultTypes.last
    lastResultType should be ("story")
  }

  test("preview_image_url should be included in the search result when available") {
    val params = Map("q" -> Seq("merry men"))
    val (_, results, _, _) = service.doSearch(params, false, AuthParams(), None, None)
    val resultPreviewImageUrls = results.results.map(_.previewImageUrl.map(_.asInstanceOf[JString].string))
    val firstPreviewImageUrl = resultPreviewImageUrls.headOption.flatten
    firstPreviewImageUrl should be (Some("https://petercetera.net/views/zeta-0007/files/123456789"))
  }

  test("preview_image_url should be None in the search result when not available") {
    val params = Map("boostFiles" -> Seq("10.0"))
    val (_, results, _, _) = service.doSearch(params, false, AuthParams(), None, None)
    val resultPreviewImageUrls = results.results.map(_.previewImageUrl.map(_.asInstanceOf[JString].string))
    val firstPreviewImageUrl = resultPreviewImageUrls.headOption.flatten
    firstPreviewImageUrl should be (None)
  }

  test("no results should come back if asked for a non-existent 4x4") {
    val params = Map("ids" -> Seq("fake-4x4"))
    val (_, results, _, _) = service.doSearch(params, false, AuthParams(), None, None)
    val actualFxfs = results.results.map(_.resource.dyn.id.!.asInstanceOf[JString].string)
    actualFxfs should be('empty)
  }

  test("adding on public=true and published=true params should not change the set of anonymously viewable results") {
    val (_, results, _, _) = service.doSearch(Map.empty, false, AuthParams(), None, None)
    val redundantParams = Map("public" -> "true", "published" -> "true").mapValues(Seq(_))
    val (_, resultsWithRedundantParams, _, _) = service.doSearch(redundantParams, false, AuthParams(), None, None)

    val actualFxfs = resultsWithRedundantParams.results.map(_.resource.dyn.id.!.asInstanceOf[JString].string)
    val expectedFxfs = results.results.map(_.resource.dyn.id.!.asInstanceOf[JString].string)
    actualFxfs should contain theSameElementsAs(expectedFxfs)
  }

  test("adding on public=false and published=false params should empty out the set of anonymously viewable results") {
    val params = Map("public" -> "false", "published" -> "false").mapValues(Seq(_))
    val (_, results, _, _) = service.doSearch(params, false, AuthParams(), None, None)
    val actualFxfs = results.results.map(_.resource.dyn.id.!.asInstanceOf[JString].string)
    actualFxfs should be('empty)
  }
}

class SearchServiceSpecWithBrokenES extends FunSuiteLike with Matchers with MockFactory {
  //  ES is broken within this class because it's not Bootstrapped
  val testSuiteName = "BrokenES"
  val client = new TestESClient(testSuiteName)
  val httpClient = new TestHttpClient()
  val coreClient = new TestCoreClient(httpClient, 8037)
  val domainClient = new DomainClient(client, coreClient, testSuiteName)
  val documentClient = new DocumentClient(client, domainClient, testSuiteName, None, None, Set.empty)
  val balboaDir = new File("balboa_test_trash")
  val balboaClient = new BalboaClient(balboaDir.getName)
  val service = new SearchService(documentClient, domainClient, balboaClient, coreClient)

  test("non fatal exceptions throw friendly error string") {
    val expectedResults = """{"error":"We're sorry. Something went wrong."}"""

    val servReq = mock[HttpServletRequest]
    servReq.expects('getMethod)().anyNumberOfTimes.returns("GET")
    servReq.expects('getRequestURI)().anyNumberOfTimes.returns("/test")
    servReq.expects('getHeaderNames)().anyNumberOfTimes.returns(Collections.emptyEnumeration[String]())
    servReq.expects('getInputStream)().anyNumberOfTimes.returns(new DelegatingServletInputStream(new ByteArrayInputStream("".getBytes)))
    servReq.expects('getHeader)(HeaderAuthorizationKey).anyNumberOfTimes.returns("Basic ricky:awesome")
    servReq.expects('getHeader)(HeaderAuthorizationKey).anyNumberOfTimes.returns("OAuth 123456789")
    servReq.expects('getHeader)(HeaderCookieKey).returns("ricky=awesome")
    servReq.expects('getHeader)(HeaderXSocrataHostKey).anyNumberOfTimes.returns(null)
    servReq.expects('getHeader)(HeaderXSocrataRequestIdKey).anyNumberOfTimes.returns("1")
    servReq.expects('getQueryString)().anyNumberOfTimes.returns("only=datasets")

    val augReq = new AugmentedHttpServletRequest(servReq)

    val httpReq = mock[HttpRequest]
    httpReq.expects('servletRequest)().anyNumberOfTimes.returning(augReq)

    val response = new MockHttpServletResponse()

    service.search(false)(httpReq)(response)
    response.getStatus should be (SC_INTERNAL_SERVER_ERROR)
    response.getHeader("Access-Control-Allow-Origin") should be ("*")
    response.getContentAsString should be (expectedResults)
  }
}
