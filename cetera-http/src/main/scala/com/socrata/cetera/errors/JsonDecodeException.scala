package com.socrata.cetera.errors

import com.rojoma.json.v3.codec.DecodeError

case class JsonDecodeException(err: DecodeError) extends RuntimeException {
  override def getMessage: String = err.english
}
