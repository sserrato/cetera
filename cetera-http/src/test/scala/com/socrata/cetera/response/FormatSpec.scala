package com.socrata.cetera.response

import scala.io.Source

import com.rojoma.json.v3.ast.{JArray, JObject, JString}
import com.rojoma.json.v3.codec.JsonDecode
import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.JsonReader
import org.scalatest._

import com.socrata.cetera.types.Datatype

class FormatSpec  extends WordSpec with ShouldMatchers {

  val drewRawString = Source.fromInputStream(getClass.getResourceAsStream("/drewRaw.json")).getLines().mkString("\n")
  val drewRawJson = JsonReader.fromString(drewRawString)

  "The hyphenize method" should {
    "return a single hyphen if given an empty string" in {
      val hyphenized = Format.hyphenize("")
      hyphenized should be("-")
    }

    "return the given string if a single alphanumeric word" in {
      val string = "hello"
      val hyphenized = Format.hyphenize(string)
      hyphenized should be(string)
    }

    "return the given string if a single alphanumeric word with unicode" in {
      val string = "Καλημέρα"
      val hyphenized = Format.hyphenize(string)
      hyphenized should be(string)
    }

    "add no hyphens to underscorized strings" in {
      val string = "hello_world"
      val hyphenized = Format.hyphenize(string)
      hyphenized should be(string)
    }

    "add hyphens where non-alphanumeric characters are present" in {
      val hyphenized1 = Format.hyphenize("hello\n\rworld")
      val hyphenized2 = Format.hyphenize("hello**world")
      val hyphenized3 = Format.hyphenize("hello world!")
      val hyphenized4 = Format.hyphenize("hello world. How ya doin'?")

      val expected1 = "hello-world"
      val expected2 = "hello-world"
      val expected3 = "hello-world-"
      val expected4 = "hello-world-How-ya-doin-"

      hyphenized1 should be(expected1)
      hyphenized2 should be(expected2)
      hyphenized3 should be(expected3)
      hyphenized4 should be(expected4)
    }

    "truncate long strings" in {
      val string = "Well hello there world!\n How are you doing on this fine gray Seattle day?"
      val hyphenized = Format.hyphenize(string)
      val expected = "Well-hello-there-world-How-are-you-doing-on-this-f"
      hyphenized should be(expected)
    }
  }

  "The links method" should {
    def testLinks(
      datatype: Option[Datatype],
      viewType: Option[String],
      category: Option[String],
      name: String,
      expectedPermaPath: String,
      expectedSeoPath: String)
    : Unit =
      s"return the correct seo and perma links for \n" +
        s"datatype=$datatype, viewType=$viewType, category=$category and name='$name'" in {
        val cname = "tempuri.org"
        val id = "1234-abcd"
        val expectedPermalink = s"https://$cname/$expectedPermaPath/$id"
        val expectedSeolink = s"https://$cname/$expectedSeoPath/$id"
        val previewImageId = "123456789"
        val expectedPreviewImageUrl = s"https://$cname/views/$id/files/$previewImageId"

        val urls = Format.links(cname, None, datatype, viewType, id, category, name, Some(previewImageId))
        urls.get("permalink").map(_.string) should be(Some(expectedPermalink))
        urls.get("link").map(_.string) should be(Some(expectedSeolink))
        urls.get("previewImageUrl").map(_.string) should be(Some(expectedPreviewImageUrl))
      }

    // datatype/viewtype tests
    val dt = "datatype"
    val vt = "viewtype"
    val xp = "expectedPermaPath"
    val xpDefault = "d"
    val xs = "expectedSeoPath"
    val xsDefault = "Public-Safety/SPD-911"

    Seq(
      Map(dt -> "calendar"),
      Map(dt -> "chart"),
      Map(dt -> "datalens", xp -> "view"),
      Map(dt -> "chart", vt -> "datalens", xp -> "view"),
      Map(dt -> "map", vt -> "datalens", xp -> "view"),
      Map(dt -> "dataset"),
      Map(dt -> "file"),
      Map(dt -> "filter"),
      Map(dt -> "form"),
      Map(dt -> "map", vt -> "geo"),
      Map(dt -> "map", vt -> "tabular"),
      Map(dt -> "href"),
      Map(dt -> "story", xp -> "stories/s", xs -> "stories/s")
    ).foreach { t =>
      val category = Some("Public Safety")
      val name = "SPD 911"
      testLinks(Datatype(t.get(dt)), t.get(vt), category, name, t.getOrElse(xp, xpDefault), t.getOrElse(xs, xsDefault))
    }

    // category tests
    Seq(None, Some("")).foreach { category =>
      val name = "this is a name"
      testLinks(Datatype("dataset"), None, category, name, "d", "dataset/this-is-a-name")
    }

    // name tests
    Seq(null, "").foreach { name =>
      val category = Some("this-is-a-category")
      testLinks(Datatype("dataset"), None, category, name, "d", "this-is-a-category/-")
    }

    // length test
    val longCategory = Some("A super long category name is not very likely but we will protect against it anyway")
    val longName = "More commonly customers may write a title that is excessively verbose and it will hit this limit"
    val limitedCategory = "A-super-long-category-name-is-not-very-likely-but-"
    val limitedName = "More-commonly-customers-may-write-a-title-that-is-"
    testLinks(Datatype("dataset"), None, longCategory, longName, "d", s"$limitedCategory/$limitedName")

    // unicode test
    val unicodeCategory = Some("بيانات عن الجدات")
    val unicodeName = "愛"
    testLinks(Datatype("dataset"), None, unicodeCategory, unicodeName, "d", "بيانات-عن-الجدات/愛")

    "return requested locale if specified" in {
      val cname = "fu.bar"
      val locale = "pirate"
      val id = "1234-abcd"
      val name = "Pirates are awesome"

      val expectedPermaLink = s"https://$cname/$locale/d/$id"
      val expectedSeoLink = s"https://$cname/$locale/dataset/${Format.hyphenize(name)}/$id"

      val urls = Format.links(cname, Some(locale), None, None, id, None, name, None)

      urls.get("permalink").get.string should be(expectedPermaLink)
      urls.get("link").get.string should be(expectedSeoLink)
    }
  }

  "the domainCategory method" should {
    "return None if passed unexpected json" in {
      val domainCategory = Format.domainCategory(j"""{"huh":"this ain't right"}""")
      domainCategory should be(None)
    }

    "return the expected JValue if passed good json" in {
      val domainCategory = Format.domainCategory(drewRawJson)
      domainCategory.get should be(JString("Finance"))
    }
  }

  "the domainCategoryString method" should {
    val esDrewString = Source.fromInputStream(getClass.getResourceAsStream("/drewRaw.json")).getLines().mkString("\n")
    val esDrewJson = JsonReader.fromString(esDrewString)

    "return None if passed unexpected json" in {
      val domainCategory = Format.domainCategoryString(j"""{"huh":"this ain't right"}""")
      domainCategory should be(None)
    }

    "return the expected string if passed good json" in {
      val domainCategory = Format.domainCategoryString(esDrewJson)
      domainCategory.get should be("Finance")
    }
  }

  "the domainTags method" should {
    "return None if passed unexpected json" in {
      val domainTags = Format.domainTags(j"""{"huh":"this ain't right"}""")
      domainTags should be(None)
    }

    "return the expected JValue if passed good json" in {
      val domainTags = Format.domainTags(drewRawJson)
      domainTags.get should be(JArray(List(JString("pie"))))
    }
  }

  "the domainMetadata method" should {
    "return None if passed unexpected json" in {
      val domainMetadata = Format.domainMetadata(j"""{"huh":"this ain't right"}""")
      domainMetadata should be(None)
    }

    "return the expected JValue if passed good json" in {
      val domainMetadata = Format.domainMetadata(drewRawJson)
      val publisherObject = JObject(Map("value" -> JString("City of Redmond"), "key" -> JString("Common-Core_Publisher")))
      val emailObject = JObject(Map("value" -> JString("drew@redmond.gov"), "key" -> JString("Common-Core_Contact-Email")))
      val metadataArray = JArray(List(publisherObject, emailObject))

      domainMetadata.get should be(metadataArray)
    }
  }

  "the categories method" should {
    "return the expected JValues if passed good json" in {
      val categories = Format.categories(drewRawJson)
      categories should be(List(JString("Finance")))
    }
  }

  "the tags method" should {
    "return the expected distinct JValues if passed good json" in {
      val tags = Format.tags(drewRawJson)
      tags should be(List(JString("finance"), JString("pie")))
    }
  }

  "the cname method" should {
    "return an empty string if passed good json but a incomplete domainId map" in {
      val cname = Format.cname(Map.empty[Int, String], drewRawJson)
      cname should be("")
    }

    "return the expected cname if passed good json and complete domainId map" in {
      val drewCname = "data.redmond.gov"
      val cname = Format.cname(Map(283 -> drewCname), drewRawJson)
      cname should be(drewCname)
    }
  }

  "the datatype method" should {
    "return None if passed unexpected json" in {
      val datatype = Format.datatype(j"""{"huh":"this ain't right"}""")
      datatype should be(None)
    }

    "return the expected cname if passed good json" in {
      val datatype = Format.datatype(drewRawJson)
      datatype.get.singular should be("chart")
    }
  }

  "the viewtype method" should {
    "return None if passed unexpected json" in {
      val viewtype = Format.viewtype(j"""{"huh":"this ain't right"}""")
      viewtype should be(None)
    }

    "return the expected cname if passed good json" in {
      val viewtype = Format.viewtype(drewRawJson)
      viewtype.get should be("pie")
    }
  }

  "the datasetId method" should {
    "return None if passed unexpected json" in {
      val datasetId = Format.datasetId(j"""{"huh":"this ain't right"}""")
      datasetId should be(None)
    }

    "return the expected cname if passed good json" in {
      val datasetId = Format.datasetId(drewRawJson)
      datasetId.get should be("94sr-dmsy")
    }
  }

  "the datasetName method" should {
    "return None if passed unexpected json" in {
      val datasetName = Format.datasetName(j"""{"huh":"this ain't right"}""")
      datasetName should be(None)
    }

    "return the expected cname if passed good json" in {
      val datasetName = Format.datasetName(drewRawJson)
      datasetName.get should be("drew")
    }
  }

  "the documentSearchResult method" should {
    "return the expected payload if passed good json" in {
      val actualResult = Format.documentSearchResult(drewRawJson, Map(283 -> "data.redmond.gov"), None, None, None).get
      val drewFormattedString = Source.fromInputStream(getClass.getResourceAsStream("/drewFormatted.json")).getLines().mkString("\n")
      val drewFormattedJson = JsonReader.fromString(drewFormattedString)

      val expectedResult = JsonDecode.fromJValue[SearchResult](drewFormattedJson).right.get
      actualResult.resource should be(expectedResult.resource)
      actualResult.link should be(expectedResult.link)
      actualResult.permalink should be(expectedResult.permalink)
      actualResult.metadata should be(expectedResult.metadata)

      // the classifications don't directly compare, since rojoma reads Lists in as Vectors
      actualResult.classification.categories(0) should be(expectedResult.classification.categories(0))
      actualResult.classification.tags(0) should be(expectedResult.classification.tags(0))
      actualResult.classification.tags(1) should be(expectedResult.classification.tags(1))
      actualResult.classification.domainCategory should be(expectedResult.classification.domainCategory)
      actualResult.classification.domainTags should be(expectedResult.classification.domainTags)
      actualResult.classification.domainMetadata should be(expectedResult.classification.domainMetadata)
    }

    "return None if passed bad json" in {
      val actualResult = Format.documentSearchResult(j"""{"bad": "json"}""", Map.empty, None, None, None)
      actualResult should be(None)
    }
  }
}
