package com.socrata

import com.socrata.http.server.responses._

package object cetera {
  val HeaderAclAllowOriginAll = Header("Access-Control-Allow-Origin", "*")

  val IndexCalendars = "calendars"
  val IndexCharts = "charts"
  val IndexDatalensCharts = "datalens_charts"
  val IndexDatalenses = "datalenses"
  val IndexDatalensMaps = "datalens_maps"
  val IndexDatasets = "datasets"
  val IndexFiles = "files"
  val IndexFilters = "filters"
  val IndexForms = "forms"
  val IndexGeoMaps = "geo_maps"
  val IndexHrefs = "hrefs"
  val IndexPulses = "pulses"
  val IndexTabularMaps = "tabular_maps"

  val Indices = List(IndexCalendars, IndexCharts, IndexDatalensCharts, IndexDatalenses, IndexDatalensMaps,
    IndexDatasets, IndexFiles, IndexFilters, IndexForms, IndexGeoMaps, IndexHrefs, IndexPulses, IndexTabularMaps)
}
