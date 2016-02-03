package com.socrata.cetera.search

import org.elasticsearch.index.query.FilterBuilders
import org.elasticsearch.search.sort.{FieldSortBuilder, SortBuilder, SortBuilders, SortOrder}

import com.socrata.cetera.types._

object Sorts {
  val sortScoreDesc: SortBuilder = {
    SortBuilders.scoreSort().order(SortOrder.DESC)
  }

  def sortFieldAsc(field: String): SortBuilder = {
    SortBuilders.fieldSort(field).order(SortOrder.ASC)
  }

  def sortFieldDesc(field: String): SortBuilder = {
    SortBuilders.fieldSort(field).order(SortOrder.DESC)
  }

  def buildAverageScoreSort(
      fieldName: String,
      rawFieldName: String,
      classifications: Set[String])
    : FieldSortBuilder = {

    SortBuilders
      .fieldSort(fieldName)
      .order(SortOrder.DESC)
      .sortMode("avg")
      .setNestedFilter(
        FilterBuilders.termsFilter(
          rawFieldName,
          classifications.toSeq: _*
        )
      )
  }

  // First pass logic is very simple. query >> categories >> tags >> default
  def chooseSort(
      searchQuery: QueryType,
      searchContext: Option[Domain],
      categories: Option[Set[String]],
      tags: Option[Set[String]])
    : SortBuilder = {

    (searchQuery, searchContext, categories, tags) match {
      // ODN Categories
      case (NoQuery, None, Some(cats), _) =>
        buildAverageScoreSort(
          CategoriesFieldType.Score.fieldName,
          CategoriesFieldType.Name.rawFieldName,
          cats
        )

      // ODN Tags
      case (NoQuery, None, None, Some(ts)) =>
        buildAverageScoreSort(
          TagsFieldType.Score.fieldName,
          TagsFieldType.Name.rawFieldName,
          ts
        )

      // Everything else
      case _ => Sorts.sortScoreDesc
    }
  }
}
