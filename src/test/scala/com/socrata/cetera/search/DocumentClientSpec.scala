package com.socrata.cetera.search

import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.JsonReader
import org.elasticsearch.action.search.SearchType.QUERY_THEN_FETCH
import org.scalatest.{BeforeAndAfterAll, ShouldMatchers, WordSpec}

import com.socrata.cetera._
import com.socrata.cetera.types._
import com.socrata.cetera.util.ValidatedQueryParameters

////////////////////////////////////////////////////////
// Brittleness deliberate. Query building not finalized.
//
// Just to reiterate: VERY BRITTLE!!!
//
// JSON does not guarantee order.

class DocumentClientSpec extends WordSpec with ShouldMatchers with BeforeAndAfterAll {
  val client = new TestESClient("esclientspec")  // Remember to close() me!!
  val documentClient: DocumentClient = DocumentClient(client, Map.empty, None, None, Set.empty)

  override protected def afterAll(): Unit = {
    client.close() // Important!!
  }

  val params = ValidatedQueryParameters(
    searchQuery = SimpleQuery("search query terms"),
    domains = Some(Set("www.example.com", "test.example.com", "socrata.com")),
    domainMetadata = None,
    searchContext = None,
    categories = Some(Set("Social Services", "Environment", "Housing & Development")),
    tags = Some(Set("taxi", "art", "clowns")),
    only = Some(Seq("datasets")),
    fieldBoosts = Map[CeteraFieldType with Boostable, Float](TitleFieldType -> 2.2f, DescriptionFieldType -> 1.1f),
    datatypeBoosts = Map.empty,
    minShouldMatch = None,
    slop = None,
    showScore = false,
    offset = 10,
    limit = 20
  )

  val shouldMatch = j"""{
    "multi_match" :
      {
        "query" : "search query terms",
        "fields" :
          [ "fts_analyzed", "fts_raw", "domain_cname" ],
        "type" : "phrase"
      }
  }"""

  val boolQuery = j"""{
    "bool" :
      {
        "must" :
          {
            "multi_match" :
              {
                "query" : "search query terms",
                "fields" :
                  [ "fts_analyzed", "fts_raw", "domain_cname" ],
                "type" : "cross_fields"
              }
          },
        "should" :
          ${shouldMatch}
      }
  }"""

  val boostedBoolQuery = j"""{
    "bool" :
      {
        "must" :
          {
            "multi_match" :
              {
                "query" : "search query terms",
                "fields" :
                  [
                    "fts_analyzed",
                    "fts_raw",
                    "domain_cname",
                    "indexed_metadata.name^2.2",
                    "indexed_metadata.description^1.1"
                  ],
                "type" : "cross_fields"
              }
          },
        "should" :
          ${shouldMatch}
      }
  }"""

  val moderationFilter = j"""{
    "not" :
      {
        "query" :
          {
            "terms" :
              {
                "moderation_status" :
                  [ "pending", "rejected" ]
              }
          }
      }
  }"""

  val customerDomainFilter = j"""{
    "not" :
      {
        "query" :
          {
            "terms" : {
                        "is_customer_domain" : [ "false" ]
                      }
          }
      }
  }"""

  val defaultFilter = j"""{
    "bool" :
      {
        "must" :
          [
            ${moderationFilter},
            ${customerDomainFilter}
          ]
      }
  }"""

  val datatypeDatasetsFilter = j"""{ "terms" : { "datatype" : [ "dataset" ] } }"""

  val domainFilter = j"""{
    "terms" :
      {
        "socrata_id.domain_cname.raw" :
          [
            "www.example.com",
            "test.example.com",
            "socrata.com"
          ]
      }
  }"""

  val animlCategoriesFilter = j"""{
    "nested" :
      {
        "query" :
          {
            "terms" :
              {
                "animl_annotations.categories.name.raw" :
                  [
                    "Social Services",
                    "Environment",
                    "Housing & Development"
                  ]
              }
          },
        "path" : "animl_annotations.categories"
      }
  }"""

  val animlTagsFilter = j"""{
    "nested" :
    {
      "query" :
        {
          "terms" :
            {
              "animl_annotations.tags.name.raw" :
                [ "taxi", "art", "clowns" ]
            }
        },
      "path" : "animl_annotations.tags"
    }
  }"""

  val complexFilter = j"""{
    "bool" :
      {
        "must" :
          [
            ${datatypeDatasetsFilter},
            ${domainFilter},
            ${moderationFilter},
            ${customerDomainFilter},
            ${animlCategoriesFilter},
            ${animlTagsFilter}
          ]
      }
  }"""

  ///////////////////
  // buildBaseRequest
  //
  "buildBaseRequest" should {
    "construct a default match all query" in {
      val expected = j"""{
        "query" :
          {
            "bool" :
              {
                "must" : { "match_all" : {} },
                "filter" :
                  ${defaultFilter}
              }
          }
      }"""

      val request = documentClient.buildBaseRequest(
        searchQuery = NoQuery,
        domains = None,
        searchContext = None,
        categories = None,
        tags = None,
        domainMetadata = None,
        only = None,
        fieldBoosts = Map.empty,
        datatypeBoosts = Map.empty,
        minShouldMatch = None,
        slop = None
      )
      val actual = JsonReader.fromString(request.toString)
      actual should be (expected)
      request.request.types should be (Array(esDocumentType))
    }

    "construct a match query with terms" in {
      val expected = j"""{
        "query" :
          {
            "bool" :
              {
                "must" :
                  ${boolQuery},
                "filter" :
                  ${defaultFilter}
              }
          }
      }"""

      val request = documentClient.buildBaseRequest(
        searchQuery = params.searchQuery,
        domains = None,
        searchContext = None,
        categories = None,
        tags = None,
        domainMetadata = None,
        only = None,
        fieldBoosts = Map.empty,
        datatypeBoosts = Map.empty,
        minShouldMatch = None,
        slop = None
      )
      val actual = JsonReader.fromString(request.toString)

      actual should be (expected)
      request.request.types should be (Array(esDocumentType))
    }

    "construct a multi match query with boosted fields" in {
      val expected = j"""{
        "query" :
          {
            "bool" :
              {
                "must" :
                  ${boostedBoolQuery},
                "filter" :
                  ${defaultFilter}
              }
          }
      }"""

      val request = documentClient.buildBaseRequest(
        searchQuery = params.searchQuery,
        domains = None,
        searchContext = None,
        categories = None,
        tags = None,
        domainMetadata = None,
        only = None,
        fieldBoosts = params.fieldBoosts,
        datatypeBoosts = Map.empty,
        minShouldMatch = None,
        slop = None
      )
      val actual = JsonReader.fromString(request.toString)

      actual should be (expected)
      request.request.types should be (Array(esDocumentType))
    }
  }


  /////////////////////
  // buildSearchRequest
  //
  "buildSearchRequest" should {
    "add from, size, sort and only to a complex base request" in {
      val expected = j"""{
        "from" : ${params.offset},
        "size" : ${params.limit},
        "query" :
          {
            "bool" :
              {
                "must" :
                  ${boolQuery},
                "filter" :
                  ${complexFilter}
              }
          },
        "sort" : [ { "_score" : {} } ]
      }"""

      val request = documentClient.buildSearchRequest(
        searchQuery = params.searchQuery,
        domains = params.domains,
        searchContext = None,
        categories = params.categories,
        tags = params.tags,
        domainMetadata = None,
        only = params.only,
        fieldBoosts = Map.empty,
        datatypeBoosts = Map.empty,
        minShouldMatch = None,
        slop = None,
        offset = params.offset,
        limit = params.limit
      )

      val actual = JsonReader.fromString(request.toString)

      actual should be (expected)
      request.toString.replaceAll("[\\s\\n]+", " ") should include(datatypeDatasetsFilter.toString())
    }

    "sort by categories score when query term is missing but cat filter is present" in {
      val expected = j"""{
        "from" : ${params.offset},
        "size" : ${params.limit},
        "query" :
          {
            "bool" :
              {
                "must" : { "match_all" : {} },
                "filter" :
                  ${complexFilter}
              }
          },
        "sort" :
          [
            {
              "animl_annotations.categories.score" :
                {
                  "order" : "desc",
                  "mode" : "avg",
                  "nested_filter" :
                    {
                      "terms" :
                        {
                          "animl_annotations.categories.name.raw" :
                            [
                              "Social Services",
                              "Environment",
                              "Housing & Development"
                            ]
                        }
                    }
                }
            }
          ]
      }"""

      val request = documentClient.buildSearchRequest(
        searchQuery = NoQuery,
        domains = params.domains,
        searchContext = None,
        categories = params.categories,
        tags = params.tags,
        domainMetadata = None,
        only = params.only,
        fieldBoosts = Map.empty,
        datatypeBoosts = Map.empty,
        minShouldMatch = None,
        slop = None,
        offset = params.offset,
        limit = params.limit
      )

      val actual = JsonReader.fromString(request.toString)

      actual should be (expected)
      request.toString.replaceAll("[\\s\\n]+", " ") should include(datatypeDatasetsFilter.toString())
    }
  }


  ////////////////////
  // buildCountRequest
  //
  "buildCountRequest" should {
    "construct a default search query with normal aggregation for domains" in {
      val expected = j"""{
        "size" : 0,
        "query" :
          {
            "bool" :
              {
                "must" : { "match_all" : {} },
                "filter" :
                  ${defaultFilter}
              }
          },
        "aggregations" :
          {
            "domains" :
              {
                "terms" :
                  {
                    "field" : "socrata_id.domain_cname.raw",
                    "size" : 0,
                    "order" : { "_count" : "desc" }
                  }
              }
          }
      }"""

      val request = documentClient.buildCountRequest(
        field = DomainFieldType,
        searchQuery = NoQuery,
        domains = None,
        searchContext = None,
        categories = None,
        tags = None,
        only = None
      )

      val actual = JsonReader.fromString(request.toString)

      actual should be (expected)
      request.request.searchType should be(QUERY_THEN_FETCH)
      request.request.contextSize should be(0)
    }

    "construct a filtered match query with nested aggregation for annotations" in {
      val expected = j"""{
        "size" : 0,
        "query" :
          {
            "bool" :
              {
                "must" :
                  ${boolQuery},
                "filter" :
                  ${complexFilter}
              }
          },
        "aggregations" :
          {
            "annotations" :
              {
                "nested" : { "path" : "animl_annotations.categories" },
                "aggregations" :
                  {
                    "names" :
                      {
                        "terms" :
                          {
                            "field" : "animl_annotations.categories.name.raw",
                            "size" : 0
                          }
                      }
                  }
              }
          }
      }"""

      val request = documentClient.buildCountRequest(
        CategoriesFieldType,
        searchQuery = params.searchQuery,
        domains = params.domains,
        searchContext = None,
        categories = params.categories,
        tags = params.tags,
        only = params.only
      )

      val actual = JsonReader.fromString(request.toString)

      actual should be (expected)
      request.request.searchType should be(QUERY_THEN_FETCH)
      request.request.contextSize should be(0)
    }
  }
}
