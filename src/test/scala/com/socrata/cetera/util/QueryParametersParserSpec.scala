package com.socrata.cetera.util

import org.scalatest.{ShouldMatchers, WordSpec}

class QueryParametersParserSpec extends WordSpec with ShouldMatchers {
  val qpp = QueryParametersParser

  // There are no real tests here but there should be.

  "QueryParametersParser" should {
    "validate a Right(Int)" in {
      qpp.validated(Right(100)) should be (100)
    }
  }
}
