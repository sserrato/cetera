package com.socrata.cetera.util

import com.rojoma.json.v3.codec.DecodeError

case class JsonDecodeException(err: DecodeError) extends RuntimeException {
  override def getMessage: String = err.english
}
