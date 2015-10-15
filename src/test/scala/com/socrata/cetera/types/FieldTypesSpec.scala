package com.socrata.cetera.types

import org.scalatest.{FunSuiteLike, Matchers}

class FieldTypesSpec extends FunSuiteLike with Matchers {
  test("raw field pathing via traits") {
    DomainFieldType.rawFieldName should be("socrata_id.domain_cname.raw")
    CategoriesFieldType.rawFieldName should be("animl_annotations.categories.name.raw")
    DomainCategoryFieldType.rawFieldName should be("customer_category.name.raw")
    DomainTagsFieldType.rawFieldName should be("customer_tags.name.raw")
    TagsFieldType.rawFieldName should be("animl_annotations.tags.name.raw")
    TitleFieldType.rawFieldName should be("indexed_metadata.name.raw")
  }
}
