package com.socrata.cetera.types

import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, JsonKeyStrategy, Strategy}

case class KeyValue(key: String, value: String)
object KeyValue {
  implicit val jCodec = AutomaticJsonCodecBuilder[KeyValue]
}

case class ValueCount(value: String, count: Long)
object ValueCount {
  implicit val jCodec = AutomaticJsonCodecBuilder[ValueCount]
}

case class FacetCount(facet: String, count: Long, values: Seq[ValueCount] = Seq.empty)
object FacetCount {
  implicit val jCodec = AutomaticJsonCodecBuilder[FacetCount]
}

@JsonKeyStrategy(Strategy.Underscore)
case class FacetHit(domainCategory: Option[String],
                    domainTags: Option[Seq[String]],
                    domainMetadata: Option[Seq[KeyValue]])
object FacetHit {
  implicit val jCodec = AutomaticJsonCodecBuilder[FacetHit]
}
