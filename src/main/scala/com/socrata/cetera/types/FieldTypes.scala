package com.socrata.cetera.types

sealed trait Groupable
sealed trait Boostable

sealed trait CeteraFieldType

// These field types are used to group counting queries
case object DomainFieldType extends CeteraFieldType with Groupable
case object CategoriesFieldType extends CeteraFieldType with Groupable
case object TagsFieldType extends CeteraFieldType with Groupable

// These field types are used for boosting (giving extra weight to fields)
case object TitleFieldType extends CeteraFieldType with Boostable
case object DescriptionFieldType extends CeteraFieldType with Boostable
