package com.socrata.cetera.types

sealed trait CeteraFieldType
case object DomainFieldType extends CeteraFieldType
case object CategoriesFieldType extends CeteraFieldType
case object TagsFieldType extends CeteraFieldType

case object TitleFieldType extends CeteraFieldType
case object DescriptionFieldType extends CeteraFieldType
