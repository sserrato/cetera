package com.socrata.cetera.types

import scala.io.Source

import org.scalatest._

class DatatypeSpec extends WordSpec with ShouldMatchers {

  def testDatatype(singular: String, plural: String, names: Set[String]): Unit =
    s"create a $singular datatype with the expected fields" in {
      val singularDatatype = Datatype(singular).get
      val pluralDatatype = Datatype(plural).get

      singularDatatype.singular should be(singular)
      singularDatatype.plural should be(plural)
      singularDatatype.names should contain theSameElementsAs(names)

      pluralDatatype.singular should be(singular)
      pluralDatatype.plural should be(plural)
      pluralDatatype.names should contain theSameElementsAs(names)
    }


  "Creating all known datatypes" should {
    val datatypes = Source.fromInputStream(getClass.getResourceAsStream("/datatypes.tsv"))
    val iter = datatypes.getLines().map(_.split("\t"))
    var numTypesTested = 0
    iter.foreach { line =>
      val singular = line(0)
      val plural = line(1)
      val names = line(2).split(",").toSet
      testDatatype(singular, plural, names)
      numTypesTested += 1
    }

    "have the same number of datatypes as the list of 'all' datatypes" in  {
      numTypesTested should be(Datatypes.all.size)
    }
  }

  "Creating an unknown datatype" should {
    "return None" in {
      val datatype = Datatype("foo")
      datatype should be(None)
    }
  }
}

