package com.socrata.cetera.types

object Datatypes {
  val materialized: Seq[Materialized] = Seq(TypeCalendars, TypeCharts,
    TypeDatalenses, TypeDatasets, TypeFiles, TypeFilters, TypeForms,
    TypeMaps, TypeHrefs, TypePulses, TypeStories)
  val renamed: Seq[Datatype] = Seq(TypeLinks)
  val viewSpecific: Seq[Datatype] = Seq(TypeDatalensCharts, TypeDatalensMaps, TypeTabularMaps)

  val all: Seq[Datatype] = viewSpecific ++ renamed ++ materialized
}

trait Datatype {
  def plural: String
  def singular: String
  def names: Seq[String]
}

object Datatype {
  def apply(s: String): Option[Datatype] = {
    Datatypes.all.find(d => d.plural == s || d.singular == s)
  }
  def apply(so: Option[String]): Option[Datatype] = {
    so.flatMap(Datatype(_))
  }
}

trait DatatypeSimple extends Datatype {
  lazy val singular: String = plural.dropRight(1)
  lazy val names: Seq[String] = Seq(singular)
}

trait DatatypeRename extends Datatype {
  lazy val singular: String = plural.dropRight(1)
}

trait Materialized extends Datatype

case object TypeCalendars extends DatatypeSimple with Materialized {
  val plural = "calendars"
}
case object TypeDatalenses extends DatatypeSimple with Materialized {
  val plural: String = "datalenses"
  override lazy val singular: String = "datalens"
}
case object TypeDatasets extends DatatypeSimple with Materialized {
  val plural: String = "datasets"
}
case object TypeFiles extends DatatypeSimple with Materialized {
  val plural: String = "files"
}
case object TypeFilters extends DatatypeSimple with Materialized {
  val plural: String = "filters"
}
case object TypeForms extends DatatypeSimple with Materialized {
  val plural: String = "forms"
}
case object TypePulses extends DatatypeSimple with Materialized {
  val plural: String = "pulses"
}
case object TypeStories extends DatatypeSimple with Materialized {
  val plural: String = "stories"
  override lazy val singular: String = "story"
}

// links -> hrefs
case object TypeHrefs extends DatatypeSimple with Materialized {
  val plural: String = "hrefs"
}
case object TypeLinks extends DatatypeRename {
  val plural: String = "links"
  override lazy val names: Seq[String] = TypeHrefs.names
}

// charts
case object TypeCharts extends DatatypeRename with Materialized {
  val plural = "charts"
  override lazy val names: Seq[String] = Seq(TypeCharts.singular, TypeDatalensCharts.singular)
}
case object TypeDatalensCharts extends DatatypeRename {
  val plural: String = "datalens_charts"
  override lazy val names: Seq[String] = TypeCharts.names
}

// maps
case object TypeMaps extends DatatypeRename with Materialized {
  val plural: String = "maps"
  override lazy val names: Seq[String] =
    Seq(TypeMaps.singular, TypeDatalensMaps.singular, TypeGeoMaps.singular, TypeTabularMaps.singular)
}
case object TypeDatalensMaps extends DatatypeRename {
  val plural: String = "datalens_maps"
  override lazy val names: Seq[String] = TypeMaps.names
}
case object TypeGeoMaps extends DatatypeRename {
  val plural: String = "geo_maps"
  override lazy val names: Seq[String] = TypeMaps.names
}
case object TypeTabularMaps extends DatatypeRename{
  val plural: String = "tabular_maps"
  override lazy val names: Seq[String] = TypeMaps.names
}
