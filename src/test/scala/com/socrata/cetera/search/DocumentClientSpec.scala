package com.socrata.cetera.search

import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.JsonReader
import org.elasticsearch.action.search.SearchType.COUNT
import org.elasticsearch.search.sort.{SortBuilders, SortOrder}
import org.scalatest.{BeforeAndAfterAll, ShouldMatchers, WordSpec}

import com.socrata.cetera.esDocumentType
import com.socrata.cetera.types._
import com.socrata.cetera.util.ValidatedQueryParameters

///////////////////////////////////////////////////////////////////////////////
// NOTE: Regarding Brittleness
//
// These tests are very brittle because they test (typically) the JSON blob
// that gets sent to ES, and JSON blobs make no guarantees about order.
//
// Some of intermediate steps that do not yet produce JSON blobs simply test
// the output of the toString function.
//
// These tests serve to codify expected behavior of our query builders.
// (e.g, given the following user input, did we build the correct sort order?)
// They do not test the correctness of those queries.
//
// Also note that a query builder is different from a request builder.

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
    fieldBoosts = Map[CeteraFieldType with Boostable, Float](
      TitleFieldType -> 2.2f,
      DescriptionFieldType -> 1.1f
    ),
    datatypeBoosts = Map.empty,
    minShouldMatch = None,
    slop = None,
    showScore = false,
    offset = 10,
    limit = 20
  )

  val shouldMatch = j"""{
    "multi_match": {
        "fields": [
            "fts_analyzed",
            "fts_raw",
            "domain_cname"
        ],
        "query": "search query terms",
        "type": "phrase"
    }
  }"""

  val boolQuery = j"""{
    "bool": {
        "must": {
            "multi_match": {
                "fields": [
                    "fts_analyzed",
                    "fts_raw",
                    "domain_cname"
                ],
                "query": "search query terms",
                "type": "cross_fields"
            }
        },
        "should": ${shouldMatch}
    }
  }"""

  val boostedBoolQuery = j"""{
    "bool": {
        "must": {
            "multi_match": {
                "fields": [
                    "fts_analyzed",
                    "fts_raw",
                    "domain_cname",
                    "indexed_metadata.name^2.2",
                    "indexed_metadata.description^1.1"
                ],
                "query": "search query terms",
                "type": "cross_fields"
            }
        },
        "should": ${shouldMatch}
    }
  }"""

  val moderationFilter = j"""{
    "not": {
        "filter": {
            "terms": {
                "moderation_status": [
                    "pending",
                    "rejected"
                ]
            }
        }
    }
  }"""

  val customerDomainFilter = j"""{
    "not": {
        "filter": {
            "terms": {
                "is_customer_domain": [
                    "false"
                ]
            }
        }
    }
  }"""

  val defaultFilter = j"""{
    "and": {
        "filters": [
            ${moderationFilter},
            ${customerDomainFilter}
        ]
    }
  }"""

  val datatypeDatasetsFilter = j"""{
    "terms": {
        "datatype": [
            "dataset"
        ]
    }
  }"""

  val domainFilter = j"""{
    "terms": {
        "socrata_id.domain_cname.raw": [
            "www.example.com",
            "test.example.com",
            "socrata.com"
        ]
    }
  }"""

  val animlCategoriesFilter = j"""{
    "nested": {
        "filter": {
            "terms": {
                "animl_annotations.categories.name.raw": [
                    "Social Services",
                    "Environment",
                    "Housing & Development"
                ]
            }
        },
        "path": "animl_annotations.categories"
    }
  }"""

  val animlTagsFilter = j"""{
    "nested": {
        "filter": {
            "terms": {
                "animl_annotations.tags.name.raw": [
                    "taxi",
                    "art",
                    "clowns"
                ]
            }
        },
        "path": "animl_annotations.tags"
    }
  }"""

  val complexFilter = j"""{
    "and": {
        "filters": [
            ${datatypeDatasetsFilter},
            ${domainFilter},
            ${moderationFilter},
            ${customerDomainFilter},
            ${animlCategoriesFilter},
            ${animlTagsFilter}
        ]
    }
  }"""


  ////////////////////////
  // buildAverageScoreSort
  ////////////////////////

  "buildAverageScoreSort" should {
    "build a sort by average field score descending" in {
      val fieldName = "this_looks_like.things"
      val rawFieldName = "this_looks_like.things.raw"
      val classifications = Set("one kind of thing", "another kind of thing")

      // Using toString as proxy since toString does not give JSON parsable string
      val expectedAsString = s"""
         |"${fieldName}"{
         |  "order" : "desc",
         |  "mode" : "avg",
         |  "nested_filter" : {
         |    "terms" : {
         |      "${rawFieldName}" : [ "${classifications.head}", "${classifications.last}" ]
         |    }
         |  }
         |}""".stripMargin

      val actual = documentClient.buildAverageScoreSort(fieldName, rawFieldName, classifications)

      actual.toString should be (expectedAsString)
    }
  }


  ///////////////////
  // buildBaseRequest
  ///////////////////

  "buildBaseRequest" should {
    "construct a default match all query" in {
      val expected = j"""{
        "query": {
            "filtered": {
                "filter": ${defaultFilter},
                "query": {
                    "match_all": {}
                }
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
        "query": {
            "filtered": {
                "filter": ${defaultFilter},
                "query": ${boolQuery}
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
        "query": {
            "filtered": {
                "filter": ${defaultFilter},
                "query": ${boostedBoolQuery}
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


  ////////////////////
  // buildCountRequest
  // /////////////////

  "buildCountRequest" should {
    "construct a default search query with normal aggregation for domains" in {
      val expected = j"""{
        "aggregations": {
            "domains": {
                "terms": {
                    "field": "socrata_id.domain_cname.raw",
                    "order": {
                        "_count": "desc"
                    },
                    "size": 0
                }
            }
        },
        "query": {
            "filtered": {
                "filter": ${defaultFilter},
                "query": {
                    "match_all": {}
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
      request.request.searchType should be (COUNT)
    }

    "construct a filtered match query with nested aggregation for annotations" in {
      val expected = j"""{
        "aggregations": {
            "annotations": {
                "aggregations": {
                    "names": {
                        "terms": {
                            "field": "animl_annotations.categories.name.raw",
                            "size": 0
                        }
                    }
                },
                "nested": {
                    "path": "animl_annotations.categories"
                }
            }
        },
        "query": {
            "filtered": {
                "filter": ${complexFilter},
                "query": ${boolQuery}
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
      request.request.searchType should be (COUNT)
    }
  }


  ////////////////////
  // buildFacetRequest
  ////////////////////

  "buildFacetRequest" should {
    val cname = "example.com"
    val expectedAsString = s"""{
      |  "size" : 0,
      |  "aggregations" : {
      |    "domain_filter" : {
      |      "filter" : {
      |        "terms" : {
      |          "socrata_id.domain_cname.raw" : [ "${cname}" ]
      |        }
      |      },
      |      "aggregations" : {
      |        "datatypes" : {
      |          "terms" : {
      |            "field" : "datatype",
      |            "size" : 0
      |          }
      |        },
      |        "categories" : {
      |          "terms" : {
      |            "field" : "customer_category.raw",
      |            "size" : 0
      |          }
      |        },
      |        "tags" : {
      |          "terms" : {
      |            "field" : "customer_tags.raw",
      |            "size" : 0
      |          }
      |        },
      |        "metadata" : {
      |          "nested" : {
      |            "path" : "customer_metadata_flattened"
      |          },
      |          "aggregations" : {
      |            "keys" : {
      |              "terms" : {
      |                "field" : "customer_metadata_flattened.key.raw",
      |                "size" : 0
      |              },
      |              "aggregations" : {
      |                "values" : {
      |                  "terms" : {
      |                    "field" : "customer_metadata_flattened.value.raw",
      |                    "size" : 0
      |                  }
      |                }
      |              }
      |            }
      |          }
      |        }
      |      }
      |    }
      |  }
      |}""".stripMargin

    val actual = documentClient.buildFacetRequest(cname)

    actual.toString should be(expectedAsString)
  }


  /////////////////////
  // buildSearchRequest
  /////////////////////

  "buildSearchRequest" should {
    "add from, size, sort and only to a complex base request" in {
      val expected = j"""{
        "from": ${params.offset},
        "query": {
            "filtered": {
                "filter": ${complexFilter},
                "query": ${boolQuery}
            }
        },
        "size": ${params.limit},
        "sort": [
            {
                "_score": {}
            }
        ]
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
        "from": ${params.offset},
        "query": {
            "filtered": {
                "filter": ${complexFilter},
                "query": {
                    "match_all": {}
                }
            }
        },
        "size": ${params.limit},
        "sort": [
            {
                "animl_annotations.categories.score": {
                    "mode": "avg",
                    "nested_filter": {
                        "terms": {
                            "animl_annotations.categories.name.raw": [
                                "Social Services",
                                "Environment",
                                "Housing & Development"
                            ]
                        }
                    },
                    "order": "desc"
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


  ////////////
  // buildSort
  ////////////

  // TODO: Really, these should check that the correct sort fns are getting
  // called, and separate tests for sort fns should test the expected strings.
  // We'll double these up for now and possibly split them out later.

  "buildSort" should {
    // Be sure that you do provide extraneous fields when possible
    "order by page views descending for default null query" in {
      val expected = SundryBuilders.sortFieldDesc("page_views.page_views_total")
      val expectedAsString = s"""
        |"page_views.page_views_total"{
        |  "order" : "desc"
        |}""".stripMargin

      expected.toString should be(expectedAsString)

      val actual = documentClient.buildSort(
        searchQuery = NoQuery,
        searchContext = None,
        categories = None,
        tags = None
      )

      actual.toString should be (expectedAsString)
    }

    "order by query score descending when given an advanced query" in {
      val expected = SundryBuilders.sortScoreDesc
      val expectedAsString = s"""
        |"_score"{ }""".stripMargin

      expected.toString should be(expectedAsString)

      val actual = documentClient.buildSort(
        searchQuery = AdvancedQuery("sandwich AND (soup OR salad)"),
        searchContext = None,
        categories = None,
        tags = None
      )

      actual.toString should be(expectedAsString)
    }

    "order by query score desc when given a simple query" in {
      val expected = SundryBuilders.sortScoreDesc
      val expectedAsString = s"""
        |"_score"{ }""".stripMargin

      expected.toString should be(expectedAsString)

      val actual = documentClient.buildSort(
        searchQuery = SimpleQuery("soup salad sandwich"),
        searchContext = None,
        categories = None,
        tags = None
      )

      actual should be(expected)
    }

    "order by average category score descending when no query but ODN categories present" in {
      val cats = Set[String]("comestibles", "potables")
      val tags = Set[String]("tasty", "sweet", "taters", "precious")

      val expectedAsString = s"""
        |"animl_annotations.categories.score"{
        |  "order" : "desc",
        |  "mode" : "avg",
        |  "nested_filter" : {
        |    "terms" : {
        |      "animl_annotations.categories.name.raw" : [ "${cats.head}", "${cats.last}" ]
        |    }
        |  }
        |}""".stripMargin

      val actual = documentClient.buildSort(
        searchQuery = NoQuery,
        searchContext = None,
        categories = Some(cats),
        tags = Some(tags)
      )

      actual.toString should be(expectedAsString)
    }

    "order by average tag score desc when no query or categories but ODN tags present" in {
      val tags = Set[String]("tasty", "sweet", "taters", "precious")
      val tagsJson = "[ \"" + tags.mkString("\", \"") + "\" ]"

      val expectedAsString = s"""
        |"animl_annotations.tags.score"{
        |  "order" : "desc",
        |  "mode" : "avg",
        |  "nested_filter" : {
        |    "terms" : {
        |      "animl_annotations.tags.name.raw" : ${tagsJson}
        |    }
        |  }
        |}""".stripMargin

      val actual = documentClient.buildSort(
        searchQuery = NoQuery,
        searchContext = None,
        categories = None,
        tags = Some(tags)
      )

      actual.toString should be(expectedAsString)
    }

    // Be sure that you do provide extraneous fields when possible
    "order by page views DESC when given no query, categories, or tags" in {
    }
  }


  ////////////////
  // generateQuery
  ////////////////

  "generateQuery" should {
    "generate a match all query by default" in {
      val expectedAsString = s"""{
        |  "match_all" : { }
        |}""".stripMargin

      val actual = documentClient.generateQuery(
        searchQuery = NoQuery,
        fieldBoosts = Map.empty,
        typeBoosts = Map.empty,
        minShouldMatch = None,
        slop = None
      )

      actual.toString should be(expectedAsString)
    }
  }
}
