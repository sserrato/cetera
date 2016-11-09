package com.socrata.cetera.errors

case class MissingRequiredParameterError(parameter: String, parameterEnglish: String) extends Throwable {
  override def getMessage: String = s"Missing required $parameterEnglish parameter: $parameter"
}
