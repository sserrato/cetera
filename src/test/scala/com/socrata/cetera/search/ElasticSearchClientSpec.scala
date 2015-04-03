package com.socrata.cetera.search

import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.JsonReader
import org.elasticsearch.action.search.SearchType.COUNT
import org.elasticsearch.node.NodeBuilder.nodeBuilder
import org.scalatest.{ShouldMatchers, WordSpec}

import com.socrata.cetera.types._
import com.socrata.cetera.util.ValidatedQueryParameters

class LocalESClient() extends ElasticSearchClient("local", 5704, "useless") {
  val node = nodeBuilder().local(true).node()
  override val client = node.client()
  override def close(): Unit = { node.close(); client.close() }
}


////////////////////////////////////////////////////////
// Brittleness deliberate. Query building not finalized.

class ElasticSearchClientSpec extends WordSpec with ShouldMatchers {

  val client = new LocalESClient()  // Remember to close() me!!

  val params = ValidatedQueryParameters(
    searchQuery = Some("search query terms"),
    domains = Set("www.example.com", "test.example.com", "socrata.com"),
    categories = Set("Social Services", "Environment", "Housing & Development"),
    tags = Set("taxi", "art", "clowns"),
    only = Some("dataset"),
    boosts = Map[CeteraFieldType, Float](TitleFieldType -> 2.2f, DescriptionFieldType -> 1.1f),
    offset = 10,
    limit = 20
  )

  val complexFilter = j"""{
    "and": {
      "filters" : [
      {
        "terms" :
        {
          "socrata_id.domain_cname.raw" : [
          "www.example.com",
          "test.example.com",
          "socrata.com"
          ]
        }
      },
      {
        "terms" :
        {
          "animl_annotations.category_names.raw" : [
          "Social Services",
          "Environment",
          "Housing & Development"
          ]
        }
      },
      {
        "terms" :
        {
          "animl_annotations.tag_names.raw" : [
          "taxi",
          "art",
          "clowns"
          ]
        }
      }
      ]
    }
  }"""

  val matchAllQuery = j"""{
    "match_all" : {}
  }"""

  val matchTermsQuery = j"""{
    "match": {
      "_all": {
        "query": ${params.searchQuery.get},
        "type": "boolean"
      }
    }
  }"""

  val multiMatchQuery = j"""{
    "multi_match" : {
      "query" : ${params.searchQuery.get},
      "fields" : [ "indexed_metadata.name^2.2", "indexed_metadata.description^1.1", "_all" ]
    }
  }"""

  val filteredQuery = j"""{
    "filtered": {
      "filter": ${complexFilter},
      "query": ${matchAllQuery}
    }
  }"""

  val complexQuery = j"""{
    "filtered": {
      "filter": ${complexFilter},
      "query": ${matchTermsQuery}
    }
  }"""


  ///////////////////
  // buildBaseRequest
  //
  "buildBaseRequest" should {
    "construct a default match all query" in {
      val expected = j"""{
        "query" : ${matchAllQuery}
      }"""

      val request = client.buildBaseRequest(
        searchQuery = None,
        domains = Set.empty,
        categories = Set.empty,
        tags = Set.empty,
        only = None,
        boosts = Map.empty
      )
      val actual = JsonReader.fromString(request.toString)

      actual should be (expected)
      request.request.types should be (Array())
    }

    "construct a match query with terms" in {
      val expected = j"""{
        "query" : ${matchTermsQuery}
      }"""

      val request = client.buildBaseRequest(
        searchQuery = params.searchQuery,
        domains = Set.empty,
        categories = Set.empty,
        tags = Set.empty,
        only = None,
        boosts = Map.empty
      )
      val actual = JsonReader.fromString(request.toString)

      actual should be (expected)
      request.request.types should be (Array())
    }

    "construct a multi match query with boosted fields" in {
      val expected = j"""{
        "query" : ${multiMatchQuery}
      }"""

      val request = client.buildBaseRequest(
        searchQuery = params.searchQuery,
        domains = Set.empty,
        categories = Set.empty,
        tags = Set.empty,
        only = None,
        boosts = params.boosts
      )
      val actual = JsonReader.fromString(request.toString)

      actual should be (expected)
      request.request.types should be (Array())
    }
  }


  /////////////////////
  // buildSearchRequest
  //
  "buildSearchRequest" should {
    "add from and size to request and set type per only" in {
      val expected = j"""{
        "from": ${params.offset},
        "size": ${params.limit},
        "query": ${matchAllQuery}
      }"""

      val request = client.buildSearchRequest(
        searchQuery = None,
        domains = Set.empty,
        categories = Set.empty,
        tags = Set.empty,
        only = params.only,
        boosts = Map.empty,
        offset = params.offset,
        limit = params.limit
      )

      val actual = JsonReader.fromString(request.toString)

      actual should be (expected)
      request.request.types should be (Array[String](params.only.get))
    }

    "add from, size, and only to a complex base request" in {
      val expected = j"""{
        "from": ${params.offset},
        "size": ${params.limit},
        "query": ${complexQuery}
      }"""

      val request = client.buildSearchRequest(
        searchQuery = params.searchQuery,
        domains = params.domains,
        categories = params.categories,
        tags = params.tags,
        only = params.only,
        boosts = Map.empty,
        offset = params.offset,
        limit = params.limit
      )

      val actual = JsonReader.fromString(request.toString)

      actual should be (expected)
      request.request.types should be (Array[String]("dataset"))
    }
  }


  ////////////////////
  // buildCountRequest
  //
  "buildCountRequest" should {
    "construct a default search query with aggregation" in {
      val expected = j"""{
        "query" : ${matchAllQuery},
        "aggregations" : {
          "counts" :
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

      val request = client.buildCountRequest(
        field = DomainFieldType,
        searchQuery = None,
        domains = Set.empty,
        categories = Set.empty,
        tags = Set.empty,
        only = None
      )

      val actual = JsonReader.fromString(request.toString)

      actual should be (expected)
      request.request.searchType should be (COUNT)
    }

    "construct a filtered match query with aggregation" in {
      val expected = j"""{
        "query" : ${complexQuery},
        "aggregations" : {
          "counts" :
          {
            "terms" :
            {
              "field" : "animl_annotations.category_names.raw",
              "size" : 0,
              "order" : { "_count" : "desc" }
            }
          }
        }
      }"""

      val request = client.buildCountRequest(
        CategoriesFieldType,
        searchQuery = params.searchQuery,
        domains = params.domains,
        categories = params.categories,
        tags = params.tags,
        only = params.only
      )

      val actual = JsonReader.fromString(request.toString)

      actual should be (expected)
      request.request.searchType should be (COUNT)
    }
  }

  client.close() // Important!!
}
