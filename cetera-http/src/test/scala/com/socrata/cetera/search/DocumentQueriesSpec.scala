package com.socrata.cetera.search

import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.JsonReader
import org.scalatest.{ShouldMatchers, WordSpec}

import com.socrata.cetera.handlers.ScoringParamSet
import com.socrata.cetera.types._

class DocumentQueriesSpec extends WordSpec with ShouldMatchers {

  val msm = Some("2<-25% 9<-3") // I can be an involved string
  val defaultMinShouldMatch = Some("37%")
  val context = Some("example.com")
  val queryString = "snuffy"
  val emptyFieldBoosts = Map.empty[CeteraFieldType with Boostable, Float]
  val fieldBoosts = Map[CeteraFieldType with Boostable, Float](TitleFieldType -> 2.2f)
  val emptyDatatypeBoosts = Map.empty[Datatype, Float]
  val datatypeBoosts = Map[Datatype, Float](Datatype("datasets").get -> 1.0f)
  val slop = Some(2)

  "DocumentQueries: simpleQuery" should {
    "return the expected query when nothing but a query is given" in {
      val simpleQuery = DocumentQueries.simpleQuery(queryString, ScoringParamSet(), None)
      val expected =j"""
        {"bool" :{
          "must" :
            {"multi_match" :{"query" : "snuffy", "fields" : [ "fts_analyzed", "fts_raw" ], "type" : "cross_fields"}},
          "should" :
            {"multi_match" :{"query" : "snuffy", "fields" : [ "fts_analyzed", "fts_raw" ], "type" : "phrase"}}
        }}"""

      val actual = JsonReader.fromString(simpleQuery.toString)
      actual should be(expected)
    }

    "return the expected query when a query and field boosts are given" in {
      val simpleQuery = DocumentQueries.simpleQuery(queryString, ScoringParamSet(fieldBoosts = fieldBoosts), None)
      val expected = j"""
        {"bool" : {
          "must" :
            {"multi_match" :{"query" : "snuffy", "fields" : [ "fts_analyzed", "fts_raw" ], "type" : "cross_fields"}},
          "should" :
            {"multi_match" :{"query" : "snuffy", "fields" : [ "fts_analyzed", "fts_raw", "indexed_metadata.name^2.2" ], "type" : "phrase"}}
        }}"""

      val actual = JsonReader.fromString(simpleQuery.toString)
      actual should be(expected)
    }

    "return the expected query when a query and datatype boosts are given" in {
      val simpleQuery = DocumentQueries.chooseMatchQuery(SimpleQuery(queryString), ScoringParamSet(datatypeBoosts = datatypeBoosts), None)
      val expected = j"""
        {"bool" : {
          "must" :
            {"multi_match" :{"query" : "snuffy", "fields" : [ "fts_analyzed", "fts_raw" ], "type" : "cross_fields"}},
          "should" :
            {"multi_match" :{"query" : "snuffy", "fields" : [ "fts_analyzed", "fts_raw" ], "type" : "phrase"}}
        }}"""

      val actual = JsonReader.fromString(simpleQuery.toString)
      actual should be(expected)
    }

    "return the expected query when a query and minShouldMatch are given" in {
      val simpleQuery = DocumentQueries.simpleQuery(queryString, ScoringParamSet(minShouldMatch = Some("2<-25% 9<-3")), None)
      val expected = j"""
        {"bool" : {
          "must" :
            {"multi_match" :{"query" : "snuffy", "fields" : [ "fts_analyzed", "fts_raw" ], "type" : "cross_fields", "minimum_should_match" : "2<-25% 9<-3"}},
          "should" :
            {"multi_match" :{"query" : "snuffy", "fields" : [ "fts_analyzed", "fts_raw" ], "type" : "phrase"}}
        }}"""

      val actual = JsonReader.fromString(simpleQuery.toString)
      actual should be(expected)
    }

    "return the expected query when a query and slop is given" in {
      val simpleQuery = DocumentQueries.simpleQuery(queryString, ScoringParamSet(slop = Some(2)), None)
      val expected =j"""
        {"bool" : {
          "must" :
            {"multi_match" :{"query" : "snuffy", "fields" : [ "fts_analyzed", "fts_raw" ], "type" : "cross_fields"}},
          "should" :
            {"multi_match" :{"query" : "snuffy", "fields" : [ "fts_analyzed", "fts_raw" ], "type" : "phrase", "slop": 2}}
        }}"""

      val actual = JsonReader.fromString(simpleQuery.toString)
      actual should be(expected)
    }

    "return the expected query when a query and all the things are given" in {
      val scoringSet = ScoringParamSet(fieldBoosts = fieldBoosts, datatypeBoosts = datatypeBoosts, minShouldMatch = msm, slop = slop)
      val simpleQuery = DocumentQueries.chooseMatchQuery(SimpleQuery(queryString), scoringSet, None)
      val expected = j"""
        {"bool" : {
          "must" :
            {"multi_match" :{"query" : "snuffy", "fields" : [ "fts_analyzed", "fts_raw" ], "type" : "cross_fields", "minimum_should_match" : "2<-25% 9<-3"}},
          "should" :
            {"multi_match" :{"query" : "snuffy", "fields" : [ "fts_analyzed", "fts_raw", "indexed_metadata.name^2.2"], "type" : "phrase", "slop": 2}}
        }}"""

      val actual = JsonReader.fromString(simpleQuery.toString)
      actual should be(expected)
    }

    "produce anything but a simple query" in {
      val expectedJson = j"""{
      "bool" :{
        "must" :{"multi_match" :{
          "query" : "query string OR (query AND string)",
          "fields" : [ "fts_analyzed", "fts_raw" ],
          "type" : "cross_fields",
          "minimum_should_match" : "20%"}},
        "should": {"multi_match" :{
            "query" : "query string OR (query AND string)",
            "fields" :[
              "fts_analyzed",
              "fts_raw",
              "indexed_metadata.description^7.77",
              "indexed_metadata.name^8.88"],
            "type" : "phrase",
            "slop" : 12}}}
      }"""

      val actual = DocumentQueries.chooseMatchQuery(
        SimpleQuery("query string OR (query AND string)"),
        ScoringParamSet(
          fieldBoosts = Map(DescriptionFieldType -> 7.77f, TitleFieldType -> 8.88f),
          datatypeBoosts = Map(DatalensDatatype -> 9.99f, DatalensMapDatatype -> 10.10f),
          minShouldMatch = Some("20%"), // minShouldMatch is a String because it can be a percentage
          slop = Some(12) // slop is max num of intervening unmatched positions permitted
        ),
        None
      )
      val actualJson = JsonReader.fromString(actual.toString)

      actualJson should be (expectedJson)
    }
  }

  "DocumentQueries: advancedQuery" should {
    "return the expected query when nothing but a query is given" in {
      val advancedQuery = DocumentQueries.advancedQuery(queryString, emptyFieldBoosts)
      val expected = j"""
        {"bool" :{"should" :[
          {"query_string" :{"query" : "snuffy","fields" : [ "fts_analyzed", "fts_raw" ],"auto_generate_phrase_queries" : true}},
          {"has_parent" :{"query" :{"query_string" :{
            "query" : "snuffy",
            "fields" : ["fts_analyzed", "fts_raw", "domain_cname"],
            "auto_generate_phrase_queries" : true}}, "parent_type" : "domain"}
        }]}}"""

      val actual = JsonReader.fromString(advancedQuery.toString)
      actual should be(expected)
    }

    "return the expected query when a query and field boosts are given" in {
      val advancedQuery = DocumentQueries.advancedQuery(queryString, fieldBoosts)
      val expected = j"""
        {"bool" :{"should" :[
          {"query_string" :{"query" : "snuffy","fields" : [ "fts_analyzed", "fts_raw", "indexed_metadata.name^2.2"],"auto_generate_phrase_queries" : true}},
          {"has_parent" :{"query" :{"query_string" :{
            "query" : "snuffy",
            "fields" : ["fts_analyzed", "fts_raw", "domain_cname", "indexed_metadata.name^2.2"],
            "auto_generate_phrase_queries" : true}}, "parent_type" : "domain"}
        }]}}"""

      val actual = JsonReader.fromString(advancedQuery.toString)
      actual should be(expected)
    }

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
      }}"""

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

  "DocumentQueries: compositeFilteredQuery" should {
    "return the expected query if no categories are given" ignore {
      // TODO:  I have a change forthcoming that simplifies this, so holding off on tests.
    }
  }

  "DocumentQueries: chooseMatchQuery" should {
    "return the expected query when no query is given" in {
      val matchQuery = DocumentQueries.chooseMatchQuery(NoQuery, ScoringParamSet(), None)
      val expected = j"""{"bool": { "must": {"match_all" :{}} } }"""

      val actual = JsonReader.fromString(matchQuery.toString)
      actual should be(expected)
    }

    "return the expected query when a simple query is given" in {
      val query = SimpleQuery(queryString)
      val simpleQuery = DocumentQueries.simpleQuery(queryString, ScoringParamSet(), None)
      val matchQuery = DocumentQueries.chooseMatchQuery(query, ScoringParamSet(), None)

      val actual = JsonReader.fromString(matchQuery.toString)
      val expected = JsonReader.fromString(simpleQuery.toString)
      actual should be(expected)
    }

    "return the expected query when an advanced query is given" in {
      val query = AdvancedQuery(queryString)
      val matchQuery = DocumentQueries.chooseMatchQuery(query, ScoringParamSet(), None)
      val advancedQuery = DocumentQueries.advancedQuery(queryString, emptyFieldBoosts)
      val actual = JsonReader.fromString(matchQuery.toString)
      val expected = JsonReader.fromString(advancedQuery.toString)
      actual should be(expected)
    }
  }

  "DocumentQueries: categoriesQuery" should {
    "return the expected query if no categories are given" in {
      val categoryQuery = DocumentQueries.categoriesQuery(None)
      categoryQuery should be(None)
    }

    "return the expected query if one category is given" in {
      val categoryQuery = DocumentQueries.categoriesQuery(Some(Set("Fun")))
      val expected = j"""
        {"nested": {"query": {"bool": {
          "should": {"match": { "animl_annotations.categories.name": { "query": "Fun", "type": "phrase" }}},
          "minimum_should_match": "1" }}, "path": "animl_annotations.categories"}
        }"""
      val actual = JsonReader.fromString(categoryQuery.get.toString)
      actual should be(expected)
    }

    "return the expected query if multiple categories are given" in {
      val categoryQuery = DocumentQueries.categoriesQuery(Some(Set("Fun", "Times")))
      val expected = j"""
        {"nested": {"query": {"bool": {"should": [
          {"match": { "animl_annotations.categories.name": { "query": "Fun", "type": "phrase" }}},
          {"match": { "animl_annotations.categories.name": { "query": "Times", "type": "phrase" }}}
         ], "minimum_should_match": "1" }}, "path": "animl_annotations.categories"}
        }"""
      val actual = JsonReader.fromString(categoryQuery.get.toString)
      actual should be(expected)
    }

    "return the expected query if one category with multiple terms is given" in {
      val categoryQuery = DocumentQueries.categoriesQuery(Some(Set("Unemployment Insurance")))
      val expected = j"""
        {"nested": {"query": {"bool": {
          "should": {"match": { "animl_annotations.categories.name": { "query": "Unemployment Insurance", "type": "phrase" }}},
          "minimum_should_match": "1" }}, "path": "animl_annotations.categories"}
      }"""
      val actual = JsonReader.fromString(categoryQuery.get.toString)
      actual should be(expected)
    }
  }

  "DocumentQueries: tagsQuery" should {
    "return the expected query if no tags are given" in {
      val tagQuery = DocumentQueries.tagsQuery(None)
      tagQuery should be(None)
    }

    "return the expected query if one tag is given" in {
      val tagQuery = DocumentQueries.tagsQuery(Some(Set("Fun")))
      val expected = j"""
        {"nested": {"query": {"bool": {
          "should": {"match": { "animl_annotations.tags.name": { "query": "Fun", "type": "phrase" }}},
          "minimum_should_match": "1" }}, "path": "animl_annotations.tags"}
        }"""
      val actual = JsonReader.fromString(tagQuery.get.toString)
      actual should be(expected)
    }

    "return the expected query if multiple tags are given" in {
      val tagQuery = DocumentQueries.tagsQuery(Some(Set("Fun", "Times")))
      val expected = j"""
        {"nested": {"query": {"bool": {"should": [
          {"match": { "animl_annotations.tags.name": { "query": "Fun", "type": "phrase" }}},
          {"match": { "animl_annotations.tags.name": { "query": "Times", "type": "phrase" }}}
        ], "minimum_should_match": "1" }}, "path": "animl_annotations.tags"}
        }"""
      val actual = JsonReader.fromString(tagQuery.get.toString)
      actual should be(expected)
    }
  }

  "DocumentQueries: domainCategoriesQuery" should {
    "return the expected query if no categories are given" in {
      val categoryQuery = DocumentQueries.domainCategoriesQuery(None)
      categoryQuery should be(None)
    }

    "return the expected query if one category is given" in {
      val categoryQuery = DocumentQueries.domainCategoriesQuery(Some(Set("Fun")))
      val expected = j"""
        {"bool": {
          "should": {"match": { "customer_category": { "query": "Fun", "type": "phrase" }}},
          "minimum_should_match": "1" }
        }"""
      val actual = JsonReader.fromString(categoryQuery.get.toString)
      actual should be(expected)
    }

    "return the expected query if multiple categories are given" in {
      val categoryQuery = DocumentQueries.domainCategoriesQuery(Some(Set("Fun", "Times")))
      val expected = j"""
        {"bool": {"should": [
          {"match": { "customer_category": { "query": "Fun", "type": "phrase" }}},
          {"match": { "customer_category": { "query": "Times", "type": "phrase" }}}
        ], "minimum_should_match": "1" }
        }"""
      val actual = JsonReader.fromString(categoryQuery.get.toString)
      actual should be(expected)
    }
  }


  "DocumentQueries: domainTagsQuery" should {
    "return the expected query if no tags are given" in {
      val tagQuery = DocumentQueries.domainTagsQuery(None)
      tagQuery should be(None)
    }

    "return the expected query if one tag is given" in {
      val tagQuery = DocumentQueries.domainTagsQuery(Some(Set("Fun")))
      val expected = j"""
        {"bool": {
          "should": {"match": { "customer_tags": { "query": "Fun", "type": "phrase" }}},
          "minimum_should_match": "1" }
        }"""
      val actual = JsonReader.fromString(tagQuery.get.toString)
      actual should be(expected)
    }

    "return the expected query if multiple tags are given" in {
      val tagQuery = DocumentQueries.domainTagsQuery(Some(Set("Fun", "Times")))
      val expected = j"""
        {"bool": {"should": [
          {"match": { "customer_tags": { "query": "Fun", "type": "phrase" }}},
          {"match": { "customer_tags": { "query": "Times", "type": "phrase" }}}
        ], "minimum_should_match": "1" }
        }"""
      val actual = JsonReader.fromString(tagQuery.get.toString)
      actual should be(expected)
    }
  }
}
