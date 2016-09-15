package com.socrata.cetera.errors

case class DomainNotFoundError(cname: String) extends Throwable {
  override def getMessage: String = s"Domain not found: $cname"
}
