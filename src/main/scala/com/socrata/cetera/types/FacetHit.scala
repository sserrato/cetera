package com.socrata.cetera.types

import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, JsonKeyStrategy, Strategy}

case class KeyValue(key: String, value: String)
object KeyValue {
  implicit val jCodec = AutomaticJsonCodecBuilder[KeyValue]
}

@JsonKeyStrategy(Strategy.Underscore)
case class FacetHit(customerCategory: Option[String],
                    customerTags: Option[Seq[String]],
                    customerMetadataFlattened: Option[Seq[KeyValue]])
object FacetHit {
  implicit val jCodec = AutomaticJsonCodecBuilder[FacetHit]
}
