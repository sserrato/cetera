package com.socrata.cetera.types

import org.scalatest.{ShouldMatchers, WordSpec}

class QueryTypeSpec extends WordSpec with ShouldMatchers {
  "minShouldMatch" should {
    "return None from a NoQuery" in {
      val query = NoQuery
      val msm = MinShouldMatch.fromParam(query, "3")
      msm should be(None)
    }

    "return None from an AdvancedQuery" in {
      val query = AdvancedQuery("anything goes here")
      val msm = MinShouldMatch.fromParam(query, "75%")
      msm should be(None)
    }

    "return Some from a SimpleQuery" in {
      val query = SimpleQuery("data about dancing shoes")
      val msm = MinShouldMatch.fromParam(query, "3<90%")
      msm should be(Some("3<90%"))
    }

    "trim whitespace from param" in {
      val query = SimpleQuery("pasta and other cards")
      val msm = MinShouldMatch.fromParam(query, " 2<-25%  9<-3   ")
      msm should be(Some("2<-25%  9<-3"))
    }
  }

  "getScriptFunction" should {
    "not have a view (singular) script" in {
      val script = ScriptScoreFunction.getScriptFunction("view")
      script match {
        case Some(_) => fail("view (singular) script should not exist")
        case None =>
      }
    }

    "have a views (plural) script" in {
      val script = ScriptScoreFunction.getScriptFunction("views")
      script match {
        case Some(_) =>
        case None => fail("expected to find a views script")
      }
    }

    "have a score (singular) script" in {
      val script = ScriptScoreFunction.getScriptFunction("score")
      script match {
        case Some(_) =>
        case None => fail("expected to find a score (singular) script")
      }
    }

    "not have a scores (plural) script" in {
      val script = ScriptScoreFunction.getScriptFunction("scores")
      script match {
        case Some(_) => fail("scores (plural) script should not exist")
        case None =>
      }
    }
  }

  "domainBoostsScoreFunction" should {
    "return an empty set when given an empty set" in {
      val domainBoosts = Map.empty[String, Float]
    }
  }
}
