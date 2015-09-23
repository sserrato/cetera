package com.socrata.cetera.util

import org.scalatest.{ShouldMatchers, WordSpec}
import com.socrata.cetera.types._

class QueryTypeSpec extends WordSpec with ShouldMatchers {
  def convertParamsToJavaDoubles(params: List[(String, Double)]): Map[String, AnyRef] =
    params.map { case (k, v) => (k, v: java.lang.Double) }.toMap

  "ScriptScoreFunction fromParam" should {
    "return a None when there is no query string" in {
      ScriptScoreFunction.fromParam(NoQuery, "popularity(1.0 2.0 3.0)") shouldBe None
    }

    "return a None when passed an AdvancedQuery" in {
      ScriptScoreFunction.fromParam(AdvancedQuery("foo"), "popularity(1.0 2.0 3.0)") shouldBe None
    }

    "return a Some when passed a SimpleQuery and a well-formed script score function string" in {
      val expected = ScriptScoreFunction(
        """weight * (doc["popularity"].value - mean) / std""",
        convertParamsToJavaDoubles(List(("mean", 1.0), ("std", 2.0), ("weight", 3.0))))

      ScriptScoreFunction.fromParam(SimpleQuery("foo"), "popularity(1.0 2.0 3.0)") shouldBe Some(expected)
    }

    "return a None when passed a SimpleQuery and a mal-formed script score function string with commas delimiting parameters rather than spaces" in {
      ScriptScoreFunction.fromParam(SimpleQuery("foo"), "popularity(1.0,2.0,3.0)") shouldBe None
    }

    "return a None when passed a SimpleQuery and a mal-formed script score function string with string parameters rather than numeric parameters" in {
      ScriptScoreFunction.fromParam(SimpleQuery("foo"), "popularity(a b c)") shouldBe None
    }
  }
}
