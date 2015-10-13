package com.socrata.cetera.types

import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, JsonKeyStrategy, Strategy}

@JsonKeyStrategy(Strategy.Underscore)
case class FacetHit(customerCategory: Option[String],
                    customerTags: Option[Seq[String]],
                    customerMetadataFlattened: Option[Map[String,String]])
object FacetHit {
  implicit val jCodec = AutomaticJsonCodecBuilder[FacetHit]
}
