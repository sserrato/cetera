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
    sharedTo: Option[String],
    attribution: Option[String],
    parentDatasetId: Option[String])

object SearchParamSet {
  def empty: SearchParamSet = SearchParamSet(NoQuery, None, None, None, None, None, None, None, None, None, None)
}

case class ScoringParamSet(
    fieldBoosts: Map[CeteraFieldType with Boostable, Float],
    datatypeBoosts: Map[Datatype, Float],
    domainBoosts: Map[String, Float],
    minShouldMatch: Option[String],
    slop: Option[Int])

object ScoringParamSet {
  def empty: ScoringParamSet = ScoringParamSet(Map.empty, Map.empty, Map.empty, None, None)
}

case class PagingParamSet(
    offset: Int,
    limit: Int,
    sortOrder: Option[String])

case class FormatParamSet(
    locale: Option[String],
    showScore: Boolean,
    showVisibility: Boolean)

object FormatParamSet {
  def empty: FormatParamSet = FormatParamSet(locale = None, showScore = false, showVisibility = false)
}

case class UserSearchParamSet(
    ids: Option[Set[String]],
    emails: Option[Set[String]],
    screenNames: Option[Set[String]],
    roles: Option[Set[String]],
    domain: Option[String],
    query: Option[String])

object UserSearchParamSet {
  def empty: UserSearchParamSet = UserSearchParamSet(None, None, None, None, None, None)
}
