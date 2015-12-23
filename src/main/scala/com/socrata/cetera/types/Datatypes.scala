package com.socrata.cetera.types

object Datatypes {
  val materialized: Seq[Materialized] = Seq(TypeCalendars, TypeCharts,
    TypeDatalenses, TypeDatasets, TypeFiles, TypeFilters, TypeForms,
    TypeMaps, TypeHrefs, TypePulses, TypeStories)
  val renamed: Seq[DatatypeSimple] = Seq(TypeLinks)
  val viewSpecific: Seq[DatatypeSimple] = Seq(TypeDatalensCharts, TypeDatalensMaps, TypeTabularMaps)

  val all: Seq[DatatypeSimple] = viewSpecific ++ renamed ++ materialized
}

trait DatatypeSimple {
  val plural: String
  lazy val singular: String = plural.dropRight(1)
  lazy val names: Seq[String] = Seq(singular)
}

object DatatypeSimple {
  def apply(s: String): Option[DatatypeSimple] = {
    Datatypes.all.find(d => d.plural == s || d.singular == s).headOption
  }
  def apply(so: Option[String]): Option[DatatypeSimple] = {
    so.flatMap(DatatypeSimple(_))
  }
}

trait DatatypeRename extends DatatypeSimple {
  override lazy val names: Seq[String] = ???
}

trait Materialized extends DatatypeSimple

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
