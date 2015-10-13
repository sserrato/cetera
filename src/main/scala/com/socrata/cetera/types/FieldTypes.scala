package com.socrata.cetera.types

sealed trait Countable
sealed trait Boostable
sealed trait Scorable
sealed trait Rawable
sealed trait CeteraFieldType

// These field types are used to group counting queries
case object DomainFieldType extends CeteraFieldType with Countable with Rawable
case object CategoriesFieldType extends CeteraFieldType with Countable with Scorable with Rawable
case object CustomerCategoryFieldType extends CeteraFieldType with Countable with Scorable with Rawable
case object TagsFieldType extends CeteraFieldType with Countable with Scorable with Rawable

// These field types are used for boosting (giving extra weight to fields)
case object TitleFieldType extends CeteraFieldType with Boostable with Rawable
case object DescriptionFieldType extends CeteraFieldType with Boostable
case object ColumnNameFieldType extends CeteraFieldType with Boostable
case object ColumnDescriptionFieldType extends CeteraFieldType with Boostable
case object ColumnFieldNameFieldType extends CeteraFieldType with Boostable
