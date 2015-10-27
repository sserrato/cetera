package com.socrata.cetera.types

import org.scalatest.{FunSuiteLike, Matchers}

class FieldTypesSpec extends FunSuiteLike with Matchers {
  test("raw field pathing via traits") {
    DomainFieldType.rawFieldName should be("socrata_id.domain_cname.raw")

    CategoriesFieldType.rawFieldName should be("animl_annotations.categories.raw")
    CategoriesFieldType.Name.rawFieldName should be("animl_annotations.categories.name.raw")
    CategoriesFieldType.Score.rawFieldName should be("animl_annotations.categories.score.raw")

    DomainCategoryFieldType.rawFieldName should be("customer_category.raw")

    DomainTagsFieldType.rawFieldName should be("customer_tags.raw")

    TagsFieldType.rawFieldName should be("animl_annotations.tags.raw")
    TagsFieldType.Name.rawFieldName should be("animl_annotations.tags.name.raw")
    TagsFieldType.Score.rawFieldName should be("animl_annotations.tags.score.raw")

    TitleFieldType.rawFieldName should be("indexed_metadata.name.raw")
  }
}
