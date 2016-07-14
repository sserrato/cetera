package com.socrata.cetera.search

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.JsonReader
import org.elasticsearch.action.search.SearchType.COUNT
import org.scalatest.{BeforeAndAfterAll, ShouldMatchers, WordSpec}

import com.socrata.cetera.handlers._
import com.socrata.cetera.types._
import com.socrata.cetera.util.ElasticsearchBootstrap
import com.socrata.cetera.{TestCoreClient, TestESClient, TestHttpClient}

///////////////////////////////////////////////////////////////////////////////
// NOTE: Regarding Brittleness
//
// These tests are very brittle because they test (typically) the JSON blob
// that gets sent to ES, and JSON blobs make no guarantees about order.
//
// These tests serve to codify expected behavior of our query builders.
// (e.g, given the following user input, did we build the correct sort order?)
// They do not test the correctness of those queries.
//
// Also note that a query builder is different from a request builder.

class DocumentClientSpec extends WordSpec with ShouldMatchers with BeforeAndAfterAll {
  val testSuiteName = getClass.getSimpleName.toLowerCase
  val esClient = new TestESClient(testSuiteName)  // Remember to close() me!!
  val httpClient = new TestHttpClient()  // Remember to close() me!!
  val coreClient = new TestCoreClient(httpClient, 2082)
  val defaultMinShouldMatch = Some("37%")
  val scriptScoreFunctions = Set(
    ScriptScoreFunction.getScriptFunction("views"),
    ScriptScoreFunction.getScriptFunction("score")
  ).flatMap { fn => fn }

  val domainClient = new DomainClient(esClient, coreClient, testSuiteName)
  val documentClient = new DocumentClient(
    esClient = esClient,
    domainClient,
    indexAliasName = testSuiteName,
    defaultTitleBoost = None,
    defaultMinShouldMatch = defaultMinShouldMatch,
    scriptScoreFunctions = scriptScoreFunctions
  )

  override protected def beforeAll(): Unit = {
    ElasticsearchBootstrap.ensureIndex(esClient, "yyyyMMddHHmm", testSuiteName)
  }

  override protected def afterAll(): Unit = {
    esClient.close() // Important!!
    httpClient.close()
  }

  val domainIds = Set(1, 2, 3)
  val searchParams = SearchParamSet(
    searchQuery = SimpleQuery("search query terms"),
    domains = Some(Set("www.example.com", "test.example.com", "socrata.com")),
    domainMetadata = None,
    searchContext = None,
    categories = Some(Set("Social Services", "Environment", "Housing & Development")),
    tags = Some(Set("taxi", "art", "clowns")),
    datatypes = Some(Set("datasets")),
    parentDatasetId = None,
    user = None,
    sharedTo = None,
    attribution = None
  )
  val scoringParams = ScoringParamSet(
    fieldBoosts = Map[CeteraFieldType with Boostable, Float](
      TitleFieldType -> 2.2f,
      DescriptionFieldType -> 1.1f
    ),
    datatypeBoosts = Map.empty,
    domainBoosts = Map.empty[String, Float],
    minShouldMatch = None,
    slop = None
  )
  val pagingParams = PagingParamSet(
    offset = 10,
    limit = 20,
    sortOrder = Option("relevance") // should be the same as None
  )
  val formatParams = FormatParamSet(
    locale = None,
    showScore = false,
    showVisibility = false
  )
  val params = ValidatedQueryParameters(searchParams, scoringParams, pagingParams, formatParams)

  def multiMatchJson(boosted: Boolean, matchType: String) = {
    val fields = if (boosted)
      j"""["fts_analyzed", "fts_raw", "indexed_metadata.name^2.2", "indexed_metadata.description^1.1"]"""
    else
      j"""["fts_analyzed", "fts_raw"]"""

    j"""{
      "multi_match": {
        "fields": $fields,
        "query": "search query terms",
        "type": $matchType
      }
    }"""
  }

  val shouldMatch = multiMatchJson(false, "phrase")
  val boostedShouldMatch = multiMatchJson(true, "phrase")

  val boolQuery = j"""{
    "bool": {
      "must": ${multiMatchJson(false, "cross_fields")},
      "should": ${shouldMatch}
    }
  }"""

  val boostedBoolQuery = j"""{
    "bool": {
      "must": ${multiMatchJson(false, "cross_fields")},
      "should": ${boostedShouldMatch}
    }
  }"""

  // NOTE: In reality, the domain_id set would be populated or no results would come back.
  // But, when domains is empty, this filter must match no (rather than all) domain_ids.
  // See instances of `domains = Set.empty[Domain]`
  val domainIdsFilterEmpty = j"""{"terms": {"socrata_id.domain_id": []}}"""
  val domainsFilter =  j"""{"terms": {"socrata_id.domain_id": [1, 2, 3]}}"""
  val defaultViewFilter = j"""{"term" : {"is_default_view" : true }}"""
  val publicFilter = j"""{"not": {"filter": {"term": { "is_public": false }}}}"""
  val publishedFilter = j"""{"not": {"filter": {"term": { "is_published": false }}}}"""
  val modApprovedFilter = j"""{"term": {"is_moderation_approved": true}}"""
  val raApprovedFilter = j"""{"term": {"is_approved_by_parent_domain": true}}"""
  val moderationFilter = j"""{"bool": { "should": [$defaultViewFilter, $modApprovedFilter]}}"""
  val routingApprovalFilter = j"""{"bool": {"must": {"bool": {"should": $raApprovedFilter}}}}"""

  val defaultFilter = j"""{
    "bool": {
      "must": [
        ${domainIdsFilterEmpty},
        ${publicFilter},
        ${publishedFilter},
        ${moderationFilter},
        ${routingApprovalFilter}]}
  }"""

  val defaultFilterPlusDomainIds = j"""{
    "bool": {
      "must": [
        ${publicFilter},
        ${publishedFilter},
        ${domainsFilter},
        ${moderationFilter},
        ${routingApprovalFilter}]}
  }"""

  val datatypeDatasetsFilter = j"""{
    "terms": {"datatype": ["dataset"]}
  }"""

  val animlCategoriesQuery = j"""{
    "nested": {
      "query": {
        "bool": {
          "should": [
            { "match": { "animl_annotations.categories.name" : { "query":"Social Services", "type":"phrase" } } },
            { "match": { "animl_annotations.categories.name" : { "query":"Environment", "type":"phrase" } } },
            { "match": { "animl_annotations.categories.name" : { "query":"Housing & Development", "type":"phrase" } } }],
          "minimum_should_match" : "1"}},
      "path": "animl_annotations.categories"}
  }"""

  val animlTagsQuery = j"""{
    "nested": {
      "query": {
        "bool": {
          "should": [
            { "match": { "animl_annotations.tags.name" : { "query":"taxi", "type":"phrase" } } },
            { "match": { "animl_annotations.tags.name" : { "query":"art", "type":"phrase" } } },
            { "match": { "animl_annotations.tags.name" : { "query":"clowns", "type":"phrase" } } }],
          "minimum_should_match" : "1"}},
      "path": "animl_annotations.tags"}
  }"""

  val noQuery = j"""{
    "bool": {
      "must": {"match_all": {}}
    }
  }"""

  val simpleQuery = j"""{
    "bool": {
      "must": [
        {"match_all": {}},
        ${animlCategoriesQuery},
        ${animlTagsQuery},
        ${domainsFilter}]}
  }"""

  val simpleQueryWithoutDomainFilter = j"""{
    "bool": {
      "must": [
        ${noQuery},
        ${animlCategoriesQuery},
        ${animlTagsQuery}]}
  }"""

  val complexQuery = j"""{
    "bool": {
      "must": [
        ${boolQuery},
        ${animlCategoriesQuery},
        ${animlTagsQuery}]}
  }"""

  val boostedComplexQuery = j"""{
    "bool": {
      "must": [
        ${boostedBoolQuery},
        ${animlCategoriesQuery},
        ${animlTagsQuery}]}
  }"""

  val complexFilter = j"""{
    "bool": {
      "must": [
        ${domainIdsFilterEmpty},
        ${datatypeDatasetsFilter},
        ${publicFilter},
        ${publishedFilter},
        ${moderationFilter},
        ${routingApprovalFilter}]}
  }"""

  val matchAll = j"""{ "match_all" : {} }"""

  def functionScoreQuery(query: JValue) = j"""{
    "function_score" : {
      "query" : ${query},
      "functions" : [
        {"script_score" : {
          "script" : "1 + doc[\"page_views.page_views_total_log\"].value",
          "lang" : "expression"}},
        {"script_score" :{ "script" : "_score", "lang" : "expression"}}],
      "score_mode" : "multiply",
      "boost_mode" : "replace"
    }
  }"""


  ////////////////////
  // buildCountRequest
  // /////////////////

  // NOTE: ultimately, the functionScoreQuery does not belong in aggregations
  "buildCountRequest" should {
    "construct a filtered match query with nested aggregation for annotations" in {
      val query = j"""{
        "filtered": {
          "query": ${complexQuery},
          "filter": ${complexFilter}
        }
      }"""

      val expected = j"""{
        "size": 0,
        "query": ${query},
        "aggregations": {"annotations": {
          "aggregations": {"names": {"terms": {"field": "animl_annotations.categories.name.raw","size": 0}}},
          "nested": {"path": "animl_annotations.categories"}}},
        "query": ${functionScoreQuery(query)}
      }"""

      val request = documentClient.buildCountRequest(CategoriesFieldType, DomainSet(Set.empty[Domain], None), searchParams, Visibility.anonymous)
      val actual = JsonReader.fromString(request.toString)

      actual should be (expected)
      request.request.searchType should be (COUNT)
    }
  }

  ////////////////////
  // buildFacetRequest
  ////////////////////

  "buildFacetRequest" should {
    "build a faceted request" in {
      val domainId = 42
      val domain = Domain(
        domainId, "", None, None,
        isCustomerDomain = true,
        moderationEnabled = false,
        routingApprovalEnabled = false,
        lockedDown = false,
        apiLockedDown = false)

      val domain42Filter =  j"""{"terms": {"socrata_id.domain_id": [42]}}"""
      val expected = j"""{
        "size" : 0,
        "aggregations" :{"domain_filter" :{
          "filter" :{"bool" :{"must" :[
            ${domain42Filter},
            $publicFilter,
            $publishedFilter,
            {"bool" :{"should" :[
              $defaultViewFilter,
              $modApprovedFilter,
              {"bool" :{"must" :[
                $domain42Filter,
                {"not" :{"filter" :{"terms" :{"datatype" :[
                  "datalens",
                  "datalens_chart",
                  "datalens_map"]}}}}]}}]}},
            {"bool" :{"must" :{"bool" :{"should" :[
              $domain42Filter,
              $raApprovedFilter]}}}}]}},
          "aggregations" :{
            "datatypes" : { "terms" : { "field" : "datatype", "size" : 0 } },
            "categories" : {"terms" : { "field" : "customer_category.raw", "size" : 0 }},
            "tags" : { "terms" : { "field" : "customer_tags.raw", "size" : 0 } },
            "metadata" :{
              "nested" : { "path" : "customer_metadata_flattened" },
              "aggregations" :{"keys" :{
                "terms" :{"field" : "customer_metadata_flattened.key.raw", "size" : 0},
                "aggregations" :{"values" :
                  {"terms" :{"field" : "customer_metadata_flattened.value.raw", "size" : 0}}}}}}}}}
      }"""

      val request = documentClient.buildFacetRequest(DomainSet(Set(domain), None), Visibility.anonymous)
      val actual = JsonReader.fromString(request.toString)

      actual should be(expected)
    }
  }

  /////////////////////
  // buildSearchRequest
  /////////////////////

  "buildSearchRequest" should {
    "add from, size, sort and only to a complex base request" in {
      val query = j"""{
        "filtered": {
          "filter": ${complexFilter},
          "query": ${complexQuery}
        }
      }"""

      val expected = j"""{
        "from": ${pagingParams.offset},
        "query": ${functionScoreQuery(query)},
        "size": ${pagingParams.limit},
        "sort": [ { "_score": {} }
        ]
      }"""

      val request = documentClient.buildSearchRequest(DomainSet.empty, searchParams, ScoringParamSet.empty, pagingParams, Visibility.anonymous)
      val actual = JsonReader.fromString(request.toString)

      actual should be (expected)
      request.toString.replaceAll("[\\s\\n]+", " ") should include(datatypeDatasetsFilter.toString())
    }

    "sort by average category scores given search context and categories" in {
      val query = j"""{
        "filtered": {
          "filter": ${complexFilter},
          "query": ${simpleQueryWithoutDomainFilter}
        }
      }"""

      val expected = j"""{
        "from": ${pagingParams.offset},
        "query": ${functionScoreQuery(query)},
        "size": ${pagingParams.limit},
        "sort": [{"animl_annotations.categories.score": {
          "mode": "avg",
          "nested_filter": {"terms": {
            "animl_annotations.categories.name.raw": ["Social Services","Environment","Housing & Development"]}},
          "order": "desc",
          "missing": "_last"}}]
      }"""

      val request = documentClient.buildSearchRequest(DomainSet.empty, searchParams.copy(searchQuery = NoQuery), ScoringParamSet.empty, pagingParams, Visibility.anonymous)
      val actual = JsonReader.fromString(request.toString)

      actual should be (expected)
      request.toString.replaceAll("[\\s\\n]+", " ") should include(datatypeDatasetsFilter.toString())
    }
  }

  ////////////////////////
  // generateAdvancedQuery
  ////////////////////////

  "generateAdvancedQuery" should {
    "produce an advanced query with field boosts applied" in {
      val expectedJson = j"""{
        "bool": {
          "should": [
            {
              "query_string": {
                "auto_generate_phrase_queries": true,
                "fields": [
                  "fts_analyzed",
                  "fts_raw",
                  "indexed_metadata.name^6.66",
                  "indexed_metadata.columns_description^1.11",
                  "indexed_metadata.columns_field_name^2.22",
                  "indexed_metadata.columns_name^3.33",
                  "datatype^4.44",
                  "indexed_metadata.description^5.55"
                ],
                "query": "any old query string"
              }
            },
            {
              "has_parent": {
                "parent_type": "domain",
                "query": {
                  "query_string": {
                    "auto_generate_phrase_queries": true,
                    "fields": [
                      "fts_analyzed",
                      "fts_raw",
                      "domain_cname",
                      "indexed_metadata.name^6.66",
                      "indexed_metadata.columns_description^1.11",
                      "indexed_metadata.columns_field_name^2.22",
                      "indexed_metadata.columns_name^3.33",
                      "datatype^4.44",
                      "indexed_metadata.description^5.55"
                    ],
                    "query": "any old query string"
                  }
                }
              }
            }
          ]
        }
      }"""

      val actual = DocumentQueries.advancedQuery(
        "any old query string",
        Map(
          ColumnDescriptionFieldType -> 1.11f,
          ColumnFieldNameFieldType -> 2.22f,
          ColumnNameFieldType -> 3.33f,
          DatatypeFieldType -> 4.44f,
          DescriptionFieldType -> 5.55f,
          TitleFieldType -> 6.66f
        )
      )

      val actualJson = JsonReader.fromString(actual.toString)

      actualJson should be (expectedJson)
    }
  }

  //////////////////////
  // generateSimpleQuery
  //////////////////////

  "generateSimpleQuery" should {
    "produce anything but a simple query" in {
      val expectedJson = j"""{
        "bool" :{
          "must" :{"multi_match" :{
            "query" : "query string OR (query AND string)",
            "fields" : [ "fts_analyzed", "fts_raw" ],
            "type" : "cross_fields",
            "minimum_should_match" : "20%"}},
          "should" :[
            {"multi_match" :{
              "query" : "query string OR (query AND string)",
              "fields" :[
                "fts_analyzed",
                "fts_raw",
                "indexed_metadata.description^7.77",
                "indexed_metadata.name^8.88"],
              "type" : "phrase",
              "slop" : 12}},
            {"term" : { "datatype" : { "value" : "datalens", "boost" : 9.99 }}},
            {"term" : { "datatype" : { "value" : "datalens_map", "boost" : 10.1 }}}]}
      }"""

      val actual = DocumentQueries.chooseMatchQuery(
        SimpleQuery("query string OR (query AND string)"),
        None,
        ScoringParamSet(
          Map(DescriptionFieldType -> 7.77f, TitleFieldType -> 8.88f), // test field boosts
          Map(TypeDatalenses -> 9.99f, TypeDatalensMaps -> 10.10f), // test type boosts
          Map.empty,
          Some("20%"), // minShouldMatch is a String because it can be a percentage
          Some(12) // slop is max num of intervening unmatched positions permitted
        ),
        None,
        None
      )

      val actualJson = JsonReader.fromString(actual.toString)

      actualJson should be (expectedJson)
    }
  }
}
