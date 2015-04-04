package com.socrata.cetera.types

sealed trait Countable
sealed trait Boostable

sealed trait CeteraFieldType

// These field types are used to group counting queries
case object DomainFieldType extends CeteraFieldType with Countable
case object CategoriesFieldType extends CeteraFieldType with Countable
case object TagsFieldType extends CeteraFieldType with Countable

// These field types are used for boosting (giving extra weight to fields)
case object TitleFieldType extends CeteraFieldType with Boostable
case object DescriptionFieldType extends CeteraFieldType with Boostable
