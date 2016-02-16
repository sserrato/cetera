package com.socrata.cetera.types

/////////////////////////////////
// Sealed Traits for Cetera Types

// A field in a document in ES
sealed trait CeteraFieldType {
  val fieldName: String
}

// A field that can be boosted (i.e, fields like title or description that can
// have extra weight given to then when matching keyword queries).
sealed trait Boostable extends CeteraFieldType

// A field that we allow to be be counted (e.g., how many documents of a given tag do we have?)
sealed trait Countable extends CeteraFieldType

// A field that we allow to be sorted on
sealed trait Sortable extends CeteraFieldType

// Used for nested fields like arrays or json blobs
sealed trait NestedField extends CeteraFieldType {
  protected val path: String
  protected lazy val keyName: String = this
    .getClass
    .getName
    .toLowerCase
    .split("\\$")
    .lastOption
    .getOrElse(throw new NoSuchElementException)

  val fieldName: String = s"$path.$keyName"
}

// These fields have a numeric score associated with them. For example, a
// document's category or tag might have a score of 0.95 to indicate that we
// are fairly confident about said category or tag. This score might be used as
// a default sort order when filtering.
sealed trait Scorable extends CeteraFieldType with Countable {
  val Name: NestedField
  val Score: NestedField
}

// For fields that have an additional "raw" version stored in ES
sealed trait Rawable extends CeteraFieldType {
  lazy val rawFieldName: String = fieldName + ".raw"
}

// For key-value things like custom metadata fields
sealed trait Mapable extends CeteraFieldType {
  val Key: NestedField
  val Value: NestedField
}


////////////////////
// Categories & Tags

// Categories have a score and a raw field name available
case object CategoriesFieldType extends Scorable with Rawable {
  val fieldName: String = "animl_annotations.categories"

  case object Name extends NestedField with Rawable {
    protected lazy val path: String = CategoriesFieldType.fieldName
  }

  case object Score extends NestedField with Rawable {
    protected lazy val path: String = CategoriesFieldType.fieldName
  }
}

// Tags have a score and a raw field name available
case object TagsFieldType extends Scorable with Rawable {
  val fieldName: String = "animl_annotations.tags"

  case object Name extends NestedField with Rawable {
    protected lazy val path: String = TagsFieldType.fieldName
  }

  case object Score extends NestedField with Rawable {
    protected lazy val path: String = TagsFieldType.fieldName
  }
}

// TODO: cetera-etl rename customer_category to domain_category
// A domain category is a domain-specific (customer-specified) category (as
// opposed to a Socrata-specific canonical category).
case object DomainCategoryFieldType extends Countable with Rawable {
  val fieldName: String = "customer_category"
}


////////////////
// Domain CNames

// Stores the domain cname associated with a document
case object DomainFieldType extends Countable with Rawable {
  val fieldName: String = "socrata_id.domain_cname"
}


/////////////////////
// Catalog Visibility

case object IsCustomerDomainFieldType extends CeteraFieldType {
  val fieldName: String = "is_customer_domain"
}

case object ModerationStatusFieldType extends CeteraFieldType {
  val fieldName: String = "moderation_status"
}

case object ApprovingDomainsFieldType extends CeteraFieldType {
  val fieldName: String = "approving_domains"
}


/////////////////////////////////////////////////
// Domain-specific Categories, Tags, and Metadata

// TODO: cetera-etl rename customer_tags to domain_tags
// Domain tags are customer-defined tags (which surface as topics in the front end).
case object DomainTagsFieldType extends Rawable {
  val fieldName: String = "customer_tags"
}

// TODO: cetera-etl rename customer_metadata_flattened to domain_metadata_flattened
// domain_metadata_flattened Domain metadata allows customers to define their
// own facets and call them whatever they like, for example you could define
// Superheroes and select from a list of them. We support these fields as
// direct url params (e.g., Superheros=Batman).
case object DomainMetadataFieldType extends Mapable with Rawable {
  val fieldName: String = "customer_metadata_flattened"

  case object Key extends NestedField with Rawable {
    protected lazy val path: String = DomainMetadataFieldType.fieldName
  }

  case object Value extends NestedField with Rawable {
    protected lazy val path: String = DomainMetadataFieldType.fieldName
  }
}


/////////////
// Boostables

case object TitleFieldType extends Boostable with Rawable with Sortable {
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

case object DatatypeFieldType extends Boostable {
  val fieldName: String = "datatype"
}


//////////////
// For sorting

// most accessed
case object PageViewsTotalFieldType extends CeteraFieldType with Sortable {
  val fieldName: String = "page_views.page_views_total"
}

// recently updated
// (not currently returned in payload to end user, probably should be)
case object UpdatedAtFieldType extends CeteraFieldType with Sortable {
  val fieldName: String = "updatedAt"
}

// frequently updated
// (not currently returned in payload to end user, probably should be)
case object UpdateFrequencyFieldType extends CeteraFieldType with Sortable {
  val fieldName: String = "update_freq"
}
