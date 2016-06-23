package com.socrata.cetera.handlers

import com.socrata.cetera.types._

case class SearchParamSet(
    searchQuery: QueryType,
    domains: Option[Set[String]],
    searchContext: Option[String],
    domainMetadata: Option[Set[(String, String)]],
    categories: Option[Set[String]],
    tags: Option[Set[String]],
    datatypes: Option[Set[String]],
    user: Option[String],
    attribution: Option[String],
    parentDatasetId: Option[String])

object SearchParamSet {
  def empty: SearchParamSet = SearchParamSet(NoQuery, None, None, None, None, None, None, None, None, None)
}

case class ScoringParamSet(
    fieldBoosts: Map[CeteraFieldType with Boostable, Float],
    datatypeBoosts: Map[Datatype, Float],
    domainBoosts: Map[String, Float],
    minShouldMatch: Option[String],
    slop: Option[Int],
    showScore: Boolean)

object ScoringParamSet {
  def empty: ScoringParamSet = ScoringParamSet(Map.empty, Map.empty, Map.empty, None, None, false)
}

case class PagingParamSet(
    offset: Int,
    limit: Int,
    sortOrder: Option[String])
