package com.socrata.cetera.handlers

import org.scalatest.{FunSuiteLike, Matchers}

import com.socrata.cetera.types._

class QueryParametersParserSpec extends FunSuiteLike with Matchers {
  val qpp = QueryParametersParser

  test("validate a Right(Int)") {
    qpp.validated(Right(100)) should be (100)
  }

  test("'only' query parameter allows blank to indicate no filter") {
    qpp.restrictParamFilterDatatype(None) should be(Right(None))
    qpp.restrictParamFilterDatatype(Some("")) should be(Right(None))
  }

  test("'only' query parameter allows all datatypes") {
    Datatypes.all.foreach { datatype =>
      qpp.restrictParamFilterDatatype(Some(datatype.plural)) should be(Right(Some(datatype.names)))
      qpp.restrictParamFilterDatatype(Some(datatype.singular)) should be(Right(Some(datatype.names)))
    }
  }

  test("'only' query parameter specific instances") {
    qpp.restrictParamFilterDatatype(Some("calendars")) should be(Right(Some(Set("calendar"))))
    qpp.restrictParamFilterDatatype(Some("datalenses")) should be(Right(Some(Set("datalens"))))
    qpp.restrictParamFilterDatatype(Some("datasets")) should be(Right(Some(Set("dataset"))))
    qpp.restrictParamFilterDatatype(Some("files")) should be(Right(Some(Set("file"))))
    qpp.restrictParamFilterDatatype(Some("filters")) should be(Right(Some(Set("filter"))))
    qpp.restrictParamFilterDatatype(Some("forms")) should be(Right(Some(Set("form"))))
    qpp.restrictParamFilterDatatype(Some("stories")) should be(Right(Some(Set("story"))))

    val linksExpected = Set("href")
    qpp.restrictParamFilterDatatype(Some("links")) should be(Right(Some(linksExpected)))

    val chartsExpected = Set("chart", "datalens_chart")
    qpp.restrictParamFilterDatatype(Some("charts")) should be(Right(Some(chartsExpected)))

    val mapsExpected = Set("datalens_map", "geo_map", "map", "tabular_map")
    qpp.restrictParamFilterDatatype(Some("maps")) should be(Right(Some(mapsExpected)))
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

    val ValidatedQueryParameters(_, scoringParams, _, _) = qpp(
      Map(
        "boostColumns" -> "10.0", // should produce 3 boosts: name, description, field_name

        "boostTitle" -> "9.0",
        "boostDesc" -> "8.0"
      ).mapValues(Seq(_)), None)

    scoringParams.fieldBoosts should be (expectedFieldBoosts)
    scoringParams.datatypeBoosts should be (Map())
  }

  test("datatype boosts are not treated as custom metadata key-value pairs") {
    val input = Map("boostCharts" -> "10.0", "foo" -> "bar").mapValues(Seq(_))
    val expected = Map("foo" -> "bar").mapValues(Seq(_))
    Params.remaining(input) should be(expected)
  }

  test("'only' query parameter allows multiple selections") {
    val params = QueryParametersParser(Map("only" -> Seq("datasets,datalenses")), None).searchParamSet
    params.datatypes should be('defined)
    params.datatypes.get should be(Set(TypeDatasets.singular, TypeDatalenses.singular))
  }

  test("'only[]' query parameter allows multiple selections") {
    val params = QueryParametersParser(Map("only[]" -> Seq("datasets", "datalenses")), None).searchParamSet
    params.datatypes should be('defined)
    params.datatypes.get should be(Set(TypeDatasets.singular, TypeDatalenses.singular))
  }

  test("no datatype boost params results in empty datatype boosts map") {
    val params = QueryParametersParser(Map("query" -> "crime").mapValues(Seq(_)), None).scoringParamset
    params.datatypeBoosts should have size 0
  }

  test("malformed datatype boost params result in empty datatype boosts map") {
    val params = QueryParametersParser(Map("query" -> "crime", "boostsDatasets" -> "5.0").mapValues(Seq(_)), None).scoringParamset
    params.datatypeBoosts should have size 0
  }

  test("well-formed datatype boost params validate") {
    val params = QueryParametersParser(Map("query" -> "crime", "boostDatasets" -> "5.0", "boostMaps" -> "2.0")
      .mapValues(Seq(_)), None).scoringParamset
    params.datatypeBoosts should have size 2
  }

  test("allow category with commas") {
    val params = QueryParametersParser(Map("categories" -> "Traffic, Parking, and Transportation")
      .mapValues(Seq(_)), None).searchParamSet
    params.categories should be('defined)
    params.categories.get should have size 1
    params.categories.get.head should be("Traffic, Parking, and Transportation")
  }

  test("allow tag with commas") {
    val params = QueryParametersParser(Map("tags" -> "this, probably, doesn't, happen, on, any, customer, sites")
      .mapValues(Seq(_)), None).searchParamSet
    params.tags should be('defined)
    params.tags.get should have size 1
    params.tags.get.head should be("this, probably, doesn't, happen, on, any, customer, sites")
  }

  test("allow multiple category parameters") {
    val params = QueryParametersParser(Map("categories" -> Seq("Traffic", "Parking", "Transportation")), None).searchParamSet
    params.categories should be('defined)
    params.categories.get should have size 3
    params.categories.get should contain theSameElementsAs Seq("Traffic", "Parking", "Transportation")
  }

  test("allow multiple tag parameters") {
    val params = QueryParametersParser(Map("tags" -> Seq("Traffic", "Parking", "Transportation")), None).searchParamSet
    params.tags should be('defined)
    params.tags.get should have size 3
    params.tags.get should contain theSameElementsAs Seq("Traffic", "Parking", "Transportation")
  }

  test("also allow categories[] parameters") {
    val params = QueryParametersParser(Map("categories" -> Seq("foo", "foos"), "categories[]" -> Seq("bar", "baz")), None).searchParamSet
    params.categories should be('defined)
    params.categories.get should have size 4
    params.categories.get should contain theSameElementsAs Seq("foo", "foos", "bar", "baz")
  }

  test("also allow tags[] parameters") {
    val params = QueryParametersParser(Map("tags" -> Seq("foo", "foos"), "tags[]" -> Seq("bar", "baz")), None).searchParamSet
    params.tags should be('defined)
    params.tags.get should have size 4
    params.tags.get should contain theSameElementsAs Seq("foo", "foos", "bar", "baz")
  }

  test("domain metadata excludes known parameters") {
    val knownEnumParams = Map(
      "only" -> Seq(
        "calendar",
        "chart",
        "datalens",
        "dataset",
        "file",
        "filter",
        "form",
        "map",
        "href",
        "pulse",
        "story",
        "link",
        "datalens_chart",
        "datalens_map",
        "tabular_map"
      ),

      "order" -> Seq(
        "relevance",
        "page_views_last_month",
        "page_views_last_week",
        "page_views_total",
        "created_at",
        "updated_at"
      )
    )

    val knownNumericParams = List(
      "boostColumns",
      "boostDesc",
      "boostTitle",
      "limit",
      "offset",
      "slop",
      "boostCalendars",
      "boostCharts",
      "boostDatalenses",
      "boostDatasets",
      "boostFiles",
      "boostFilters",
      "boostForms",
      "boostMaps",
      "boostHrefs",
      "boostPulses",
      "boostStories",
      "boostLinks",
      "boostDatalens_charts",
      "boostDatalens_maps",
      "boostTabular_maps"
    ).map(p => p -> Seq("42")).toMap

    val knownStringParams = List(
      "search_context",
      "domains",
      "categories",
      "tags",
      "q",
      "q_internal",
      "function_score",
      "min_should_match",
      "for_user",
      "shared_to"
    ).map(p => p -> Seq(p)).toMap

    val knownBooleanParams = List(
      "show_feature_vals",
      "show_score",
      "show_visibility",
      "public",
      "published"
    ).map(p => p -> Seq("true")).toMap

    val knownArrayParams = List(
      "categories[]",
      "tags[]"
    ).map(p => p -> Seq(p)).toMap

    // This is how they show up from socrata-http
    val domainBoostExamples = List(
      "boostDomains[example.com]",
      "boostDomains[data.seattle.gov]",
      "boostDomains[xyz]",
      "boostDomains[]"
    ).map(p => p -> Seq("1.23"))

    QueryParametersParser(
      knownEnumParams ++
        knownNumericParams ++
        knownStringParams ++
        knownArrayParams ++
        domainBoostExamples,
      None
    ).searchParamSet.domainMetadata shouldNot be('defined)
  }

  // just documenting that we do not support boostDomains without the []
  test ("boostDomains without [] gets interpreted as custom metadata") {
    val params = QueryParametersParser(Map("boostDomains" -> Seq("1.23"), "pants" -> Seq("2.34")), None).searchParamSet
    params.domainMetadata match {
      case Some(metadata) => metadata should be(Set("boostDomains" -> "1.23", "pants" -> "2.34"))
      case None => fail("expected to see boostDomains show up in metadata")
    }
  }

  // empty query string param is passed in from socrata-http multi params sometimes, e.g. catalog?q=bikes&
  test("empty query string param key doesn't throw") {
    QueryParametersParser(Map("" -> Seq()), None)
  }

  // empty query string param is passed in from socrata-http multi params sometimes, e.g. catalog?q=bikes&one+extra
  test("empty param values shouldn't throw") {
    QueryParametersParser(Map("one extra" -> Seq()), None)
    QueryParametersParser(Map("q" -> Seq("")), None)
    QueryParametersParser(Map("q_internal" -> Seq("")), None)
    QueryParametersParser(Map("domains" -> Seq("")), None)
  }

  test("empty boolean param values should be true") {
    QueryParametersParser(Map("show_score" -> Seq()), None).formatParamSet.showScore should be(true)
    QueryParametersParser(Map("show_visibility" -> Seq()), None).formatParamSet.showVisibility should be(true)
    QueryParametersParser(Map("public" -> Seq()), None).searchParamSet.public.get should be(true)
    QueryParametersParser(Map("published" -> Seq()), None).searchParamSet.published.get should be(true)
    QueryParametersParser(Map("derived" -> Seq()), None).searchParamSet.derived.get should be(true)
    QueryParametersParser(Map("explicitly_hidden" -> Seq()), None).searchParamSet.explicitlyHidden.get should be(true)
  }

  test("explicitly true boolean param values should be true") {
    QueryParametersParser(Map("show_score" -> Seq("true")), None).formatParamSet.showScore should be(true)
    QueryParametersParser(Map("show_visibility" -> Seq("true")), None).formatParamSet.showVisibility should be(true)
    QueryParametersParser(Map("public" -> Seq("true")), None).searchParamSet.public.get should be(true)
    QueryParametersParser(Map("published" -> Seq("true")), None).searchParamSet.published.get should be(true)
    QueryParametersParser(Map("derived" -> Seq("true")), None).searchParamSet.derived.get should be(true)
    QueryParametersParser(Map("explicitly_hidden" -> Seq("true")), None).searchParamSet.explicitlyHidden.get should be(true)
  }

  test("explicitly false boolean param values should be false") {
    QueryParametersParser(Map("show_score" -> Seq("false")), None).formatParamSet.showScore should be(false)
    QueryParametersParser(Map("show_visibility" -> Seq("false")), None).formatParamSet.showVisibility should be(false)
    QueryParametersParser(Map("public" -> Seq("false")), None).searchParamSet.public.get should be(false)
    QueryParametersParser(Map("published" -> Seq("false")), None).searchParamSet.published.get should be(false)
    QueryParametersParser(Map("derived" -> Seq("false")), None).searchParamSet.derived.get should be(false)
    QueryParametersParser(Map("explicitly_hidden" -> Seq("false")), None).searchParamSet.explicitlyHidden.get should be(false)
  }

  test("empty non-boolean param values shouldn't be defined") {
    QueryParametersParser(Map("search_context" -> Seq("")), None).searchParamSet.searchContext shouldNot be('defined)
    QueryParametersParser(Map("categories" -> Seq("")), None).searchParamSet.categories shouldNot be('defined)
    QueryParametersParser(Map("tags" -> Seq("")), None).searchParamSet.tags shouldNot be('defined)
    QueryParametersParser(Map("datatypes" -> Seq("")), None).searchParamSet.datatypes shouldNot be('defined)
    QueryParametersParser(Map("for_user" -> Seq("")), None).searchParamSet.user shouldNot be('defined)
    QueryParametersParser(Map("shared_to" -> Seq("")), None).searchParamSet.sharedTo shouldNot be('defined)
    QueryParametersParser(Map("attribution" -> Seq("")), None).searchParamSet.attribution shouldNot be('defined)
    QueryParametersParser(Map("derived_from" -> Seq("")), None).searchParamSet.parentDatasetId shouldNot be('defined)
    QueryParametersParser(Map("custom_metadata" -> Seq("")), None).searchParamSet.parentDatasetId shouldNot be('defined)
    QueryParametersParser.prepUserParams(Map("role" -> Seq(""))).searchParamSet.roles shouldNot be('defined)
    QueryParametersParser.prepUserParams(Map("email" -> Seq(""))).searchParamSet.emails shouldNot be('defined)
    QueryParametersParser.prepUserParams(Map("screen_name" -> Seq(""))).searchParamSet.screenNames shouldNot be('defined)
    QueryParametersParser.prepUserParams(Map("domain" -> Seq(""))).searchParamSet.domain shouldNot be('defined)
  }

  test("domain boosts can be parsed") {
    val domainBoosts = Map(
      "boostDomains[example.com]" -> Seq("1.23"),
      "boostDomains[data.seattle.gov]" -> Seq("4.56")
    )

    QueryParametersParser(domainBoosts, None).scoringParamset.domainBoosts should be(
      Map("example.com" -> 1.23f, "data.seattle.gov" -> 4.56f)
    )
  }

  test("domain boost defined twice will be completely ignored -- just documenting behavior") {
    val domainBoosts = Map(
      "boostDomains[example.com]" -> Seq("1.23", "2.34"),
      "boostDomains[data.seattle.gov]" -> Seq("4.56")
    )

    QueryParametersParser(domainBoosts, None).scoringParamset.domainBoosts should be(Map("data.seattle.gov" -> 4.56f))
  }

  test("domain boosts missing fields do not explode the params parser") {
    val domainBoosts = Map(
      "boostDomains[data.seattle.gov]" -> Seq("4.56"),
      "boostDomains[example.com]" -> Seq(),
      "boostDomains[]" -> Seq("7.89"),
      "boostDomains[]" -> Seq()
    )

    QueryParametersParser(domainBoosts, None).scoringParamset.domainBoosts should be(Map("data.seattle.gov" -> 4.56f))
  }

  test("domain boost degenerate cases do not explode the params parser") {
    val domainBoosts = Map(
      "boostDomains[boostDomains[example.com]]" -> Seq("1.23")
    )

    QueryParametersParser(domainBoosts, None).scoringParamset.domainBoosts should be(Map("boostDomains[example.com]" -> 1.23f))
  }

  test("domain boost params are not interpreted as custom metadata fields") {
    val params = Map(
      Params.searchContext -> Seq("example.com"),
      Params.boostDomains + "[example.com]" -> Seq("1.23")
    )

    QueryParametersParser(params, None).scoringParamset.domainBoosts should be(Map("example.com" -> 1.23f))
  }

  // just documenting current if not-quite-ideal behavior
  test("boostDomains without [] is not supported and gets interpreted as metadata") {
    val params = Map(
      Params.searchContext -> Seq("example.com"),
      Params.boostDomains + "example.com" -> Seq("1.23"),
      Params.boostDomains -> Seq("1.23")
    )
    QueryParametersParser(params, None).scoringParamset.domainBoosts should be(Map.empty[String, Float])
  }

  test("sort order can be parsed") {
    val sortOrder = Map("order" -> Seq("page_views_total"))
    QueryParametersParser(sortOrder, None).pagingParamSet.sortOrder should be(Some("page_views_total"))
  }

  test("an upper bound of 10000 is imposed on the limit parameter") {
    val limit = Map("limit" -> Seq("100000"))
    QueryParametersParser(limit, None).pagingParamSet.limit should be(10000)
  }

  test("a limit param of less than 10000 is allowed") {
    val limit = Map("limit" -> Seq("100"))
    QueryParametersParser(limit, None).pagingParamSet.limit should be(100)
  }
 }

class ParamsSpec extends FunSuiteLike with Matchers {
  test("isCatalogKey can recognize string keys") {
    val keys = Seq("search_context", "only", "domains", "slop", "boostMaps")
    keys.foreach { key =>
      Params.isCatalogKey(key) should be (true)
      Params.isCatalogKey(key.reverse) should be (false)
     }
  }

  test("isCatalogKey can recognize array keys") {
    val keys = Seq("categories[]", "tags[]")
    keys.foreach { key =>
      Params.isCatalogKey(key) should be (true)
      Params.isCatalogKey("phony_" + key) should be (false)
    }
  }

  test("isCatalogKey can recognize hashmap keys like boostDomains[]") {
    val keys = Seq("boostDomains[example.com]", "boostDomains[data.seattle.gov]")
    keys.foreach { key =>
      Params.isCatalogKey(key) should be (true)
      Params.isCatalogKey("phony_" + key) should be (false)
    }
  }

  test("isCatalogKey does not recognize boostDomains without []") {
    Params.isCatalogKey("boostDomains") should be (false)
  }

  test("isCatalogKey recognized sort order") {
    Params.isCatalogKey("order") should be (true)
  }

  test("isCatalogKey recognizes attribution") {
    Params.isCatalogKey("attribution") should be (true)
  }
}
