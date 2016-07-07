package com.socrata.cetera.search

case class Visibility(
    authenticationRequired: Boolean,
    publicOnly: Boolean,
    publishedOnly: Boolean,
    approvedOnly: Boolean,
    moderatedOnly: Boolean)

object Visibility {
  val anonymous = Visibility(
    authenticationRequired = false,
    publicOnly = true,
    publishedOnly = true,
    approvedOnly = true,
    moderatedOnly = true)

  val full = Visibility(
    authenticationRequired = true,
    publicOnly = false,
    publishedOnly = false,
    approvedOnly = false,
    moderatedOnly = false)
}
