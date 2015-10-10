package com.socrata.cetera.types

// These field types are used to group counting queries
sealed trait Countable extends CeteraFieldType

// These field types are used for boosting (giving extra weight to fields)
sealed trait Boostable extends CeteraFieldType

sealed trait Scorable extends CeteraFieldType with Countable {
  def scoreFieldName: String = fieldName + ".score"
  override val rawExtension: String = ".name.raw"
}

sealed trait Rawable extends CeteraFieldType {
  def rawFieldName: String = fieldName + rawExtension
}

sealed trait CeteraFieldType {
  val fieldName: String
  val rawExtension: String = ".raw"
}

case object DomainFieldType extends Countable with Rawable {
  val fieldName: String = "socrata_id.domain_cname"
}

case object CategoriesFieldType extends Scorable with Rawable {
  val fieldName: String = "animl_annotations.categories"
}

case object CustomerCategoryFieldType extends Scorable with Rawable {
  val fieldName: String = "customer_category"
}

case object CustomerTagsFieldType extends Scorable {
  val fieldName: String = "customer_tags"
}

case object CustomerMetadataBubbleupPartialFieldType extends Scorable {
  val fieldName: String = "customer_metadata_bubbleup"
}

case object CustomerMetadataFlattenedPartialFieldType extends Scorable {
  val fieldName: String = "customer_metadata_flattened"
}

case object TagsFieldType extends Scorable with Rawable {
  val fieldName: String = "animl_annotations.tags"
}

case object TitleFieldType extends Boostable with Rawable {
  val fieldName: String = "indexed_metadata.name"
}

case object DescriptionFieldType extends Boostable {
  val fieldName: String = "indexed_metadata.description"
}

case object ColumnNameFieldType extends Boostable {
  val fieldName: String = "indexed_metadata.columns_name"
}

case object ColumnDescriptionFieldType extends Boostable {
  val fieldName: String = "indexed_metadata.columns_description"
}

case object ColumnFieldNameFieldType extends Boostable {
  val fieldName: String = "indexed_metadata.columns_field_name"
}
