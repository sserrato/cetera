package com.socrata.cetera.types

// These field types are used to group counting queries
sealed trait Countable extends CeteraFieldType

// These field types are used for boosting (giving extra weight to fields)
sealed trait Boostable extends CeteraFieldType

sealed trait Scorable extends CeteraFieldType with Countable {
  val Name: NestedField
  val Score: NestedField
}

sealed trait Mapable extends CeteraFieldType with Countable {
  val Key: NestedField
  val Value: NestedField
}

sealed trait NestedField extends CeteraFieldType with Countable {
  protected val path: String
  protected lazy val keyName: String = this.getClass.getName.toLowerCase.split("\\$").last
  val fieldName: String = s"$path.$keyName"
}

sealed trait Rawable extends CeteraFieldType {
  lazy val rawFieldName: String = fieldName + ".raw"
}

sealed trait CeteraFieldType {
  val fieldName: String
}

case object DomainFieldType extends Countable with Rawable {
  val fieldName: String = "socrata_id.domain_cname"
}

case object IsCustomerDomainFieldType extends CeteraFieldType {
  val fieldName: String = "is_customer_domain"
}

case object ModerationStatusFieldType extends CeteraFieldType {
  val fieldName: String = "moderation_status"
}

case object CategoriesFieldType extends Scorable with Rawable {
  val fieldName: String = "animl_annotations.categories"
  case object Name extends NestedField with Rawable {
    protected lazy val path: String = CategoriesFieldType.fieldName
  }
  case object Score extends NestedField with Rawable {
    protected lazy val path: String = CategoriesFieldType.fieldName
  }
}

// TODO: cetera-etl rename customer_blah to domain_blah
case object DomainCategoryFieldType extends Countable with Rawable {
  val fieldName: String = "customer_category"
}

case object DomainTagsFieldType extends Countable with Rawable {
  val fieldName: String = "customer_tags"
}

case object DomainMetadataFieldType extends Mapable with Rawable {
  val fieldName: String = "customer_metadata_flattened"
  case object Key extends NestedField with Rawable {
    protected lazy val path: String = DomainMetadataFieldType.fieldName
  }
  case object Value extends NestedField with Rawable {
    protected lazy val path: String = DomainMetadataFieldType.fieldName
  }
}

case object TagsFieldType extends Scorable with Rawable {
  val fieldName: String = "animl_annotations.tags"
  case object Name extends NestedField with Rawable {
    protected lazy val path: String = TagsFieldType.fieldName
  }
  case object Score extends NestedField with Rawable {
    protected lazy val path: String = TagsFieldType.fieldName
  }
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

case object PageViewsTotalFieldType extends CeteraFieldType {
  val fieldName: String = "page_views.page_views_total"
}

case object TypeFieldType extends Boostable {
  val fieldName: String = "_type"
}
