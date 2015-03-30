package com.socrata.cetera.types

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.interpolation._

case class Countable(thing: JValue, count: JValue)

object Countable {
  def encode(label: String) = new JsonEncode[Countable] {
    def encode(x: Countable) = {
      j"""{ $label : ${x.thing}, count: ${x.count} }"""
    }
  }
}

