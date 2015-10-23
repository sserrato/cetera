package com.socrata.cetera.util

import org.scalatest.{ShouldMatchers, WordSpec}
import com.socrata.cetera.types._

class QueryTypeSpec extends WordSpec with ShouldMatchers {
  def convertParamsToJavaDoubles(params: List[(String, Double)]): Map[String, AnyRef] =
    params.map { case (k, v) => (k, v: java.lang.Double) }.toMap

  "ScriptScoreFunction fromParam" should {
    "ignore script function and return None when there is no query string" in {
      ScriptScoreFunction.fromParam(NoQuery, "nonExistantScriptFn(1.0 2.0 3.0)") shouldBe None
    }

    "ignore script function and return None when passed an AdvancedQuery" in {
      ScriptScoreFunction.fromParam(AdvancedQuery("foo"), "nonExistantScriptFn(1.0 2.0 3.0)") shouldBe None
    }

    "parse a SimpleQuery with script score function string and return expected script score fn" in {
      val expected = ScriptScoreFunction(
        """weight * (doc["page_views.page_views_total"].value - mean) / std""",
        convertParamsToJavaDoubles(List(("mean", 1.0), ("std", 2.0), ("weight", 3.0))))

      ScriptScoreFunction.fromParam(SimpleQuery("foo"), "views(1.0 2.0 3.0)") shouldBe Some(expected)
    }

    "return a None when passed a SimpleQuery and a mal-formed script score function string with commas delimiting parameters rather than spaces" in {
      ScriptScoreFunction.fromParam(SimpleQuery("foo"), "popularity(1.0,2.0,3.0)") shouldBe None
    }

    "return a None when passed a SimpleQuery and a mal-formed script score function string with string parameters rather than numeric parameters" in {
      ScriptScoreFunction.fromParam(SimpleQuery("foo"), "popularity(a b c)") shouldBe None
    }
  }
}
