package com.socrata.cetera.types

import com.rojoma.json.v3.ast.{JNumber, JString, JValue}
import com.rojoma.json.v3.codec.JsonEncode
import com.rojoma.json.v3.interpolation._

case class Count(thing: JValue, count: JValue)

object Count {
  def encode(label: String): JsonEncode[Count] = {
    new JsonEncode[Count] {
      def encode(x: Count): JValue = {
        j"""{
          $label : ${x.thing}, count: ${x.count}
        }"""
      }
    }
  }

  def apply(thing: String, count: Int): Count = {
    Count(JString(thing), JNumber(count))
  }
}
