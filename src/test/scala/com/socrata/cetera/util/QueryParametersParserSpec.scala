package com.socrata.cetera.util

import com.socrata.cetera.types._
import org.scalatest.{FunSuiteLike, Matchers}

class QueryParametersParserSpec extends FunSuiteLike with Matchers {
  val qpp = QueryParametersParser

  test("validate a Right(Int)") {
    qpp.validated(Right(100)) should be (100)
  }

  test("'only' query parameter allows all datatypes") {
    Datatypes.all.foreach { datatype =>
      qpp.restrictParamFilterType(Some(datatype.plural)) should be(Right(Some(datatype.names)))
      qpp.restrictParamFilterType(Some(datatype.singular)) should be(Right(Some(datatype.names)))
    }
  }

  test("'only' query paramter prohibits multiple selections") {
    qpp.restrictParamFilterType(Option("datasets,datalenses")) match {
      case Left(e: OnlyError) => ()
      case _ => fail("an OnlyError should be returned")
    }
  }
}
