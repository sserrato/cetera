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

    (searchQuery, categories, tags) match {
      // Query
      case (AdvancedQuery(_) | SimpleQuery(_), _, _) => Sorts.sortScoreDesc

      // ODN Categories
      // Q: What happens when we search according to domain_categories? It's not clear
      case (_, Some(cats), _) if searchContext.isEmpty =>
        buildAverageScoreSort(
          CategoriesFieldType.Score.fieldName,
          CategoriesFieldType.Name.rawFieldName,
          cats
        )

      // ODN Tags
      case (_, _, Some(ts)) if searchContext.isEmpty =>
        buildAverageScoreSort(
          TagsFieldType.Score.fieldName,
          TagsFieldType.Name.rawFieldName,
          ts
        )

      // Default (No query, categories, or tags)
      case (_, _, _) =>
        Sorts.sortFieldDesc(PageViewsTotalFieldType.fieldName)
    }
  }

}
