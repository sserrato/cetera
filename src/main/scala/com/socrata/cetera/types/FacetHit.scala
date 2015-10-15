package com.socrata.cetera.types

import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, JsonKeyStrategy, Strategy}

case class KeyValue(key: String, value: String)
object KeyValue {
  implicit val jCodec = AutomaticJsonCodecBuilder[KeyValue]
}

case class FacetCount(facet: String, count: Long)
object FacetCount {
  implicit val jCodec = AutomaticJsonCodecBuilder[FacetCount]
}

case class ValueCount(value: String, count: Long)
object ValueCount {
  implicit val jCodec = AutomaticJsonCodecBuilder[ValueCount]
}

@JsonKeyStrategy(Strategy.Underscore)
case class FacetHit(customerCategory: Option[String],
                    customerTags: Option[Seq[String]],
                    customerMetadataFlattened: Option[Seq[KeyValue]])
object FacetHit {
  implicit val jCodec = AutomaticJsonCodecBuilder[FacetHit]
}
