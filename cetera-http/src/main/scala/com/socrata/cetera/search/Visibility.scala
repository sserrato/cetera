package com.socrata.cetera.search

case class Visibility(
    authenticationRequired: Boolean,
    allPrivateVisible: Boolean,
    publicOnly: Boolean,
    publishedOnly: Boolean,
    approvedOnly: Boolean,
    moderatedOnly: Boolean)

object Visibility {
  val anonymous = Visibility(
    authenticationRequired = false,
    allPrivateVisible = false,
    publicOnly = true,
    publishedOnly = true,
    approvedOnly = true,
    moderatedOnly = true)

  val assetSelector = Visibility(
    authenticationRequired = true,
    allPrivateVisible = false,
    publicOnly = false,
    publishedOnly = false,
    approvedOnly = true,
    moderatedOnly = true)

  val personalCatalog = Visibility(
    authenticationRequired = true,
    allPrivateVisible = false,
    publicOnly = false,
    publishedOnly = false,
    approvedOnly = false,
    moderatedOnly = false)

  val full = Visibility(
    authenticationRequired = true,
    allPrivateVisible = true,
    publicOnly = false,
    publishedOnly = false,
    approvedOnly = false,
    moderatedOnly = false)
}
