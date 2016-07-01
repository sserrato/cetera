package com.socrata.cetera.search

import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.JsonReader
import org.scalatest.{ShouldMatchers, WordSpec}

import com.socrata.cetera.handlers.ScoringParamSet
import com.socrata.cetera.types._

class QueriesSpec extends WordSpec with ShouldMatchers {

  val msm = Some("2<-25% 9<-3") // I can be an involved string
  val defaultMinShouldMatch = Some("37%")
  val context = Some("example.com")
  val queryString = "snuffy"
  val emptyFieldBoosts = Map.empty[CeteraFieldType with Boostable, Float]
  val fieldBoosts = Map[CeteraFieldType with Boostable, Float](TitleFieldType -> 2.2f)
  val emptyDatatypeBoosts = Map.empty[Datatype, Float]
  val datatypeBoosts = Map[Datatype, Float](Datatype("datasets").get -> 1.0f)
  val slop = Some(2)

  "DocumentQueries: chooseMinShouldMatch" should {
    "choose minShouldMatch if present" in {
      DocumentQueries.chooseMinShouldMatch(msm, None, None) should be(msm)
      DocumentQueries.chooseMinShouldMatch(msm, defaultMinShouldMatch, None) should be(msm)
      DocumentQueries.chooseMinShouldMatch(msm, defaultMinShouldMatch, context) should be(msm)
    }

    // Use case here is increasing search precision on customer domains
    "choose default MSM value if none is passed in but search context is present" in {
      DocumentQueries.chooseMinShouldMatch(None, defaultMinShouldMatch, context) should be(defaultMinShouldMatch)
    }

    "choose nothing if no MSM value is passed in and no search context is present" in {
      DocumentQueries.chooseMinShouldMatch(None, defaultMinShouldMatch, None) should be(None)
    }
  }

  "DocumentQueries: simpleQuery" should {
    "return the expected query when nothing but a query is given" in {
      val simpleQuery = DocumentQueries.simpleQuery(queryString, emptyFieldBoosts, None, None)
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
      val simpleQuery = DocumentQueries.simpleQuery(queryString, fieldBoosts, None, None)
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
      val simpleQuery = DocumentQueries.chooseMatchQuery(SimpleQuery(queryString), None, ScoringParamSet(Map.empty, datatypeBoosts, Map.empty, None, None, showScore = false), None, None)
      val expected = j"""
        {"bool" : {
          "must" :
            {"multi_match" :{"query" : "snuffy", "fields" : [ "fts_analyzed", "fts_raw" ], "type" : "cross_fields"}},
          "should" : [
            {"multi_match" :{"query" : "snuffy", "fields" : [ "fts_analyzed", "fts_raw" ], "type" : "phrase"}},
            {"term" : {"datatype" : {"value" : "dataset", "boost" : 1.0}}}]
        }}"""

      val actual = JsonReader.fromString(simpleQuery.toString)
      actual should be(expected)
    }

    "return the expected query when a query and minShouldMatch are given" in {
      val simpleQuery = DocumentQueries.simpleQuery(queryString, emptyFieldBoosts, msm, None)
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
      val simpleQuery = DocumentQueries.simpleQuery(queryString, emptyFieldBoosts, None, slop)
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
      val simpleQuery = DocumentQueries.chooseMatchQuery(SimpleQuery(queryString), None, ScoringParamSet(fieldBoosts, datatypeBoosts, Map.empty, msm, slop, showScore = false), None, None)
      val expected = j"""
        {"bool" : {
          "must" :
            {"multi_match" :{"query" : "snuffy", "fields" : [ "fts_analyzed", "fts_raw" ], "type" : "cross_fields", "minimum_should_match" : "2<-25% 9<-3"}},
          "should" : [
            {"multi_match" :{"query" : "snuffy", "fields" : [ "fts_analyzed", "fts_raw", "indexed_metadata.name^2.2"], "type" : "phrase", "slop": 2}},
            {"term" : {"datatype" : {"value" : "dataset", "boost" : 1.0}}}]
        }}"""

      val actual = JsonReader.fromString(simpleQuery.toString)
      actual should be(expected)
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
  }

  "DocumentQueries: compositeFilteredQuery" should {
    "return the expected query if no categories are given" ignore {
      // TODO:  I have a change forthcoming that simplifies this, so holding off on tests.
    }
  }

  "DocumentQueries: chooseMatchQuery" should {
    "return the expected query when no query is given" in {
      val matchQuery = DocumentQueries.chooseMatchQuery(NoQuery, None, ScoringParamSet.empty, None, None)
      val expected = j"""{"bool": { "must": {"match_all" :{}} } }"""

      val actual = JsonReader.fromString(matchQuery.toString)
      actual should be(expected)
    }

    "return the expected query when a simple query is given" in {
      val query = SimpleQuery(queryString)
      val simpleQuery = DocumentQueries.simpleQuery(queryString, emptyFieldBoosts, None, None)
      val matchQuery = DocumentQueries.chooseMatchQuery(query, None, ScoringParamSet.empty, None, None)

      val actual = JsonReader.fromString(matchQuery.toString)
      val expected = JsonReader.fromString(simpleQuery.toString)
      actual should be(expected)
    }

    "return the expected query when an advanced query is given" in {
      val query = AdvancedQuery(queryString)
      val matchQuery = DocumentQueries.chooseMatchQuery(query, None, ScoringParamSet.empty, None, None)
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
