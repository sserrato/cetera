package com.socrata.cetera.util

import com.socrata.cetera.types._
import org.scalatest.{FunSuiteLike, Matchers}
import Matchers._

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

  test("'only' query parameter specific instances") {
    qpp.restrictParamFilterType(Some("calendars")) should be(Right(Some(Seq("calendar"))))
    qpp.restrictParamFilterType(Some("datalenses")) should be(Right(Some(Seq("datalens"))))
    qpp.restrictParamFilterType(Some("datasets")) should be(Right(Some(Seq("dataset"))))
    qpp.restrictParamFilterType(Some("files")) should be(Right(Some(Seq("file"))))
    qpp.restrictParamFilterType(Some("filters")) should be(Right(Some(Seq("filter"))))
    qpp.restrictParamFilterType(Some("forms")) should be(Right(Some(Seq("form"))))
    qpp.restrictParamFilterType(Some("stories")) should be(Right(Some(Seq("story"))))

    val linksExpected = Seq("href")
    qpp.restrictParamFilterType(Some("links")) should be(Right(Some(linksExpected)))

    val chartsExpected = Seq("chart", "datalens_chart")
    qpp.restrictParamFilterType(Some("charts")) match {
      case Right(Some(ss)) => chartsExpected.foreach(t => ss should contain(t))
      case _ => fail()
    }

    val mapsExpected = Seq("datalens_map", "geo_map", "map", "tabular_map")
    qpp.restrictParamFilterType(Some("maps")) match {
      case Right(Some(ss)) => mapsExpected.foreach(t => ss should contain(t))
      case _ => fail()
    }
  }

  test("datatype boost parameter parses with valid datatype") {
    Params.datatypeBoostParam("boostDatasets") should contain(TypeDatasets)
  }

  test("datatype boost parameter ignore with invalid datatype") {
    Params.datatypeBoostParam("boostFoo") shouldBe empty
  }

  test("datatype boost parameter parsing is case-sensitive") {
    Params.datatypeBoostParam("BOOSTDATASETS") shouldBe empty
    Params.datatypeBoostParam("boostdatasets") shouldBe empty
    Params.datatypeBoostParam("boostDatasets") should contain(TypeDatasets)
  }

  test("field boosts are treated separately from datatype boosts") {
    val expectedFieldBoosts = Map(
        ColumnNameFieldType -> 10.0,
        ColumnDescriptionFieldType -> 10.0,
        ColumnFieldNameFieldType -> 10.0,
        TitleFieldType -> 9.0,
        DescriptionFieldType -> 8.0)

    val vqps = qpp(Map("boostColumns" -> "10.0", "boostTitle" -> "9.0", "boostDesc" -> "8.0"))

    vqps match {
      case Left(_) => fail("a ValidatedQueryParameters should be returned")
      case Right(_) =>
    }

    vqps.right.get.fieldBoosts should be (expectedFieldBoosts)
    vqps.right.get.datatypeBoosts should be (Map())
  }

  test("datatype boosts are not treated as custom metadata key-value pairs") {
    Params.remaining(Map("boostCharts" -> "10.0", "foo" -> "bar")) should be (Map("foo" -> "bar"))
  }

  test("'only' query parameter prohibits multiple selections") {
    qpp.restrictParamFilterType(Option("datasets,datalenses")) match {
      case Left(e: OnlyError) => ()
      case _ => fail("an OnlyError should be returned")
    }
  }

  test("no datatype boost params results in empty datatype boosts map") {
    QueryParametersParser(Map("query" -> "crime")) match {
      case Right(params) => params.datatypeBoosts should have size 0
      case _ => fail()
    }
  }

  test("malformed datatype boost params result in empty datatype boosts map") {
    QueryParametersParser(Map("query" -> "crime", "boostsDatasets" -> "5.0")) match {
      case Right(params) => params.datatypeBoosts should have size 0
      case _ => fail()
    }
  }

  test("well-formed datatype boost params validate") {
    QueryParametersParser(Map("query" -> "crime", "boostDatasets" -> "5.0", "boostMaps" -> "2.0")) match {
      case Right(params) => params.datatypeBoosts should have size 2
      case _ => fail()
    }
  }
}
