package com.socrata.cetera.search
import com.socrata.cetera.types.CeteraFieldType
import com.socrata.cetera.types._

object EnrichedFieldTypesForES {
  def baseFieldName(field:CeteraFieldType): String = {
    field match {
      case DomainFieldType => "socrata_id.domain_cname"

      case CategoriesFieldType => "animl_annotations.categories"
      case TagsFieldType => "animl_annotations.tags"

      case TitleFieldType => "indexed_metadata.name"
      case DescriptionFieldType => "indexed_metadata.description"
    }
  }

  implicit class FieldTypeToFieldName(field: CeteraFieldType) {
    def fieldName:String = baseFieldName(field)
  }


  implicit class FieldTypeToRawFieldName(field: CeteraFieldType with Countable) {
    def rawFieldName: String = {
      field match {
        case DomainFieldType => baseFieldName(field)+".raw"
        case CategoriesFieldType => baseFieldName(field) + ".name.raw"
        case TagsFieldType => baseFieldName(field) + ".name.raw"
      }
    }
  }
  // NOTE: domain_cname does not currently have a score associated with it
  implicit class FieldTypeToScoreFieldName(field: CeteraFieldType with Countable with Scorable) {
    def scoreFieldName: String = baseFieldName(field) + ".score"
  }


}
