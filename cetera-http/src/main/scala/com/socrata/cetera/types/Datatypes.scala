package com.socrata.cetera.types

object Datatypes {
  val materialized: Seq[Materialized] = Seq(TypeCalendars, TypeCharts,
    TypeDatalenses, TypeDatasets, TypeFiles, TypeFilters, TypeForms,
    TypeMaps, TypeLinks, TypePulses, TypeStories, TypeApis)
  val renamed: Seq[Datatype] = Seq(TypeHrefs, TypeFederatedHrefs)
  val viewSpecific: Seq[Datatype] = Seq(TypeDatalensCharts, TypeDatalensMaps, TypeTabularMaps)

  val all: Seq[Datatype] = viewSpecific ++ renamed ++ materialized
}

trait Datatype {
  def plural: String
  def singular: String
  def names: Set[String]
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
  lazy val names: Set[String] = Set(singular)
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
  val allVarieties = Set(singular, TypeDatalensCharts.singular, TypeDatalensMaps.singular)
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
case object TypeApis extends DatatypeSimple with Materialized {
  val plural: String = "apis"
}

// links -> hrefs
case object TypeLinks extends DatatypeRename with Materialized {
  val plural: String = "links"
  override lazy val names: Set[String] = Set(TypeHrefs.singular, TypeFederatedHrefs.singular)
}
case object TypeHrefs extends DatatypeRename {
  val plural: String = "hrefs"
  override lazy val names: Set[String] = TypeLinks.names
}
case object TypeFederatedHrefs extends DatatypeRename {
  val plural: String = "federated_hrefs"
  override lazy val names: Set[String] = TypeLinks.names
}

// charts
case object TypeCharts extends DatatypeRename with Materialized {
  val plural = "charts"
  override lazy val names: Set[String] = Set(TypeCharts.singular, TypeDatalensCharts.singular)
}
case object TypeDatalensCharts extends DatatypeRename {
  val plural: String = "datalens_charts"
  override lazy val names: Set[String] = TypeCharts.names
}

// maps
case object TypeMaps extends DatatypeRename with Materialized {
  val plural: String = "maps"
  override lazy val names: Set[String] =
    Set(TypeMaps.singular, TypeDatalensMaps.singular, TypeGeoMaps.singular, TypeTabularMaps.singular)
}
case object TypeDatalensMaps extends DatatypeRename {
  val plural: String = "datalens_maps"
  override lazy val names: Set[String] = TypeMaps.names
}
case object TypeGeoMaps extends DatatypeRename {
  val plural: String = "geo_maps"
  override lazy val names: Set[String] = TypeMaps.names
}
case object TypeTabularMaps extends DatatypeRename{
  val plural: String = "tabular_maps"
  override lazy val names: Set[String] = TypeMaps.names
}
