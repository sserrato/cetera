package com.socrata.cetera.types

object Datatypes {
  val all: Seq[Datatype] = Seq(ApiDatatype, CalendarDatatype, ChartDatatype,
    DatalensChartDatatype, DatalensDatatype, DatalensMapDatatype, DatasetDatatype,
    FederatedHrefDatatype, FileDatatype, FilterDatatype, FormDatatype, GeoMapDatatype, HrefDatatype,
    LinkDatatype, MapDatatype, PulseDatatype, StoryDatatype, TabularMapDatatype, VisualizationDatatype)
}

// Datatypes support search via the `only` param. Either the `singular` or `plural` variants
// of the type may be passed in.  Note that Datatypes only require `plural` to be defined, so be
// careful to check that the predefined `singular` and `names` definitions do as you intend.
// The `singular` values are what are ultimately used in search. These should match the datatype values in ES.
// The `names` values allow you to group datatypes. For example, only=maps searches across several varieties of
// maps, so you'll find its `names` definition including multiple types.
trait Datatype {
  def plural: String
  def singular: String = plural.dropRight(1)
  def names: Set[String] = Set(singular)
}

object Datatype {
  def apply(s: String): Option[Datatype] = {
    Datatypes.all.find(d => d.plural == s || d.singular == s)
  }
  def apply(so: Option[String]): Option[Datatype] = {
    so.flatMap(Datatype(_))
  }
}

// alphabetized list of known datatypes
case object ApiDatatype extends Datatype {
  override val plural: String = "apis"
}
case object CalendarDatatype extends Datatype {
  override val plural = "calendars"
}
case object ChartDatatype extends Datatype {
  override val plural = "charts"
  override val names: Set[String] = Set(ChartDatatype.singular, DatalensChartDatatype.singular)
}
case object DatalensChartDatatype extends Datatype {
  override val plural: String = "datalens_charts"
}
case object DatalensDatatype extends Datatype {
  override val plural: String = "datalenses"
  override val singular: String = "datalens"
  // only=datalens includes only datalens, but for the purposes of moderation filtering,
  // all datalens varieties include regular datalens as well as DL maps & charts
  val allVarieties = Set(singular, DatalensChartDatatype.singular, DatalensMapDatatype.singular)
}
case object DatalensMapDatatype extends Datatype {
  override val plural: String = "datalens_maps"
}
case object DatasetDatatype extends Datatype {
  override val plural: String = "datasets"
}
case object FederatedHrefDatatype extends Datatype {
  override val plural: String = "federated_hrefs"
}
case object FileDatatype extends Datatype {
  override val plural: String = "files"
}
case object FilterDatatype extends Datatype {
  override val plural: String = "filters"
}
case object FormDatatype extends Datatype {
  override val plural: String = "forms"
}
case object GeoMapDatatype extends Datatype {
  override val plural: String = "geo_maps"
}
case object HrefDatatype extends Datatype {
  override val plural: String = "hrefs"
}
case object LinkDatatype extends Datatype {
  override val plural: String = "links"
  override val names: Set[String] = Set(HrefDatatype.singular, FederatedHrefDatatype.singular)
}
case object MapDatatype extends Datatype {
  override val plural: String = "maps"
  override val names: Set[String] =
    Set(MapDatatype.singular, DatalensMapDatatype.singular, GeoMapDatatype.singular, TabularMapDatatype.singular)
}
case object PulseDatatype extends Datatype {
  override val plural: String = "pulses"
}
case object StoryDatatype extends Datatype {
  override val plural: String = "stories"
  override val singular: String = "story"
}
case object TabularMapDatatype extends Datatype{
  override val plural: String = "tabular_maps"
}
case object VisualizationDatatype extends Datatype {
  override val plural: String = "visualizations"
}
