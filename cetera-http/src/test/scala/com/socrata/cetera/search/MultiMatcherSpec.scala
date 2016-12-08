package com.socrata.cetera.search

import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.JsonReader
import org.elasticsearch.index.query.MultiMatchQueryBuilder.Type.{CROSS_FIELDS, PHRASE}
import org.scalatest.{BeforeAndAfterAll, ShouldMatchers, WordSpec}

import com.socrata.cetera.TestESDomains
import com.socrata.cetera.auth.User
import com.socrata.cetera.handlers.ScoringParamSet

class MultiMatcherSpec extends WordSpec with ShouldMatchers with BeforeAndAfterAll with TestESDomains {
  "buildQuery" should {
    val q = "some query string"

    "create a cross fields query over public fields if no user is passed" in {
      val actual = JsonReader.fromString(MultiMatchers.buildQuery(q, CROSS_FIELDS, ScoringParamSet(), None).toString)
      val expected =
        j"""{
        "multi_match" :
          {
            "query" : "some query string",
            "fields" : [ "fts_analyzed", "fts_raw" ],
            "type" : "cross_fields"
          }
      }"""
      actual should be(expected)
    }

    "create a phrase query over public fields if no user is passed" in {
      val actual = JsonReader.fromString(MultiMatchers.buildQuery(q, PHRASE, ScoringParamSet(), None).toString)
      val expected =
        j"""{
        "multi_match" :
          {
            "query" : "some query string",
            "fields" : [ "fts_analyzed", "fts_raw" ],
            "type" : "phrase"
          }
      }"""
      actual should be(expected)
    }

    "create a cross fields query over public fields and owned/shared items in private fields if the user is roleless" in {
      val user = User("annabelle", authenticatingDomain = Some(domains(0)))
      val actual = JsonReader.fromString(MultiMatchers.buildQuery(q, CROSS_FIELDS, ScoringParamSet(), Some(user)).toString)
      val expected =
        j"""
        { "bool" : { "should" : [
          { "multi_match" : {
            "query" : "some query string",
            "fields" : [ "fts_analyzed", "fts_raw" ],
            "type" : "cross_fields"
          }},
          {"filtered" :
            { "query" :
              {"multi_match" : {
                "query" : "some query string",
                "fields" : ["private_fts_analyzed", "private_fts_raw"],
                "type" : "cross_fields"
              }},
            "filter" : { "bool" : { "should" :[
              { "term" : { "owner_id" : "annabelle" } },
              { "term" : { "shared_to" : "annabelle" } }
            ]}}
          }}
        ]}}"""
      actual should be(expected)
    }

    "create a phrase query over public fields and owned/shared items in private fields if the user is roleless" in {
      val user = User("annabelle", authenticatingDomain = Some(domains(0)))
      val actual = JsonReader.fromString(MultiMatchers.buildQuery(q, PHRASE, ScoringParamSet(), Some(user)).toString)
      val expected =
        j"""
        { "bool" : { "should" : [
          { "multi_match" : {
            "query" : "some query string",
            "fields" : [ "fts_analyzed", "fts_raw" ],
            "type" : "phrase"
          }},
          {"filtered" :
            { "query" :
              {"multi_match" : {
                "query" : "some query string",
                "fields" : ["private_fts_analyzed", "private_fts_raw"],
                "type" : "phrase"
              }},
            "filter" : { "bool" : { "should" :[
              { "term" : { "owner_id" : "annabelle" } },
              { "term" : { "shared_to" : "annabelle" } }
            ]}}
          }}
        ]}}"""
      actual should be(expected)
    }

    "create a cross fields query over all fields for a super admin" in {
      val user = User("annabelle", flags = Some(List("admin")))
      val actual = JsonReader.fromString(MultiMatchers.buildQuery(q, CROSS_FIELDS, ScoringParamSet(), Some(user)).toString)
      val expected =
        j"""{
          "multi_match" :
            {
              "query" : "some query string",
              "fields" : [ "fts_analyzed", "fts_raw", "private_fts_analyzed", "private_fts_raw" ],
              "type" : "cross_fields"
            }
        }"""
      actual should be(expected)
    }

    "create a phrase query over all fields for a super admin" in {
      val user = User("annabelle", flags = Some(List("admin")))
      val actual = JsonReader.fromString(MultiMatchers.buildQuery(q, PHRASE, ScoringParamSet(), Some(user)).toString)
      val expected =
        j"""{
          "multi_match" :
            {
              "query" : "some query string",
              "fields" : [ "fts_analyzed", "fts_raw", "private_fts_analyzed", "private_fts_raw" ],
              "type" : "phrase"
            }
        }"""
      actual should be(expected)
    }

    "create a cross fields query over public fields and all domain/owned/shared items in private fields if the user has an enabling role" in {
      val user = User("annabelle", authenticatingDomain = Some(domains(0)), roleName = Some("administrator"))
      val actual = JsonReader.fromString(MultiMatchers.buildQuery(q, CROSS_FIELDS, ScoringParamSet(), Some(user)).toString)
      val expected =
        j"""
          { "bool" : { "should" : [
            { "multi_match" : {
              "query" : "some query string",
              "fields" : [ "fts_analyzed", "fts_raw" ],
              "type" : "cross_fields"
            }},
            {"filtered" :
              { "query" :
                {"multi_match" : {
                  "query" : "some query string",
                  "fields" : ["private_fts_analyzed", "private_fts_raw"],
                  "type" : "cross_fields"
                }},
              "filter" : { "bool" : { "should" :[
                { "term" : { "owner_id" : "annabelle" } },
                { "term" : { "shared_to" : "annabelle" } },
                { "terms" : { "socrata_id.domain_id" : [ 0 ] } }
              ]}}
            }}
          ]}}"""
      actual should be(expected)
    }

    "create a phrase query over public fields and all domain/owned/shared items in private fields if the user has an enabling role" in {
      val user = User("annabelle", authenticatingDomain = Some(domains(0)), roleName = Some("administrator"))
      val actual = JsonReader.fromString(MultiMatchers.buildQuery(q, PHRASE, ScoringParamSet(), Some(user)).toString)
      val expected =
        j"""
          { "bool" : { "should" : [
            { "multi_match" : {
              "query" : "some query string",
              "fields" : [ "fts_analyzed", "fts_raw" ],
              "type" : "phrase"
            }},
            {"filtered" :
              { "query" :
                {"multi_match" : {
                  "query" : "some query string",
                  "fields" : ["private_fts_analyzed", "private_fts_raw"],
                  "type" : "phrase"
                }},
              "filter" : { "bool" : { "should" :[
                { "term" : { "owner_id" : "annabelle" } },
                { "term" : { "shared_to" : "annabelle" } },
                { "terms" : { "socrata_id.domain_id" : [ 0 ] } }
              ]}}
            }}
          ]}}"""
      actual should be(expected)
    }
  }
}
