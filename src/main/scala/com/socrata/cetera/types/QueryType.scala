package com.socrata.cetera.types

sealed trait QueryType

case object NoQuery extends QueryType

case class SimpleQuery(query:String) extends QueryType

case class AdvancedQuery(query:String) extends QueryType
