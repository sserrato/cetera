package com.socrata.cetera.search

case class Visibility(
    authenticationRequired: Boolean,
    alsoIncludeLoggedInUserOwned: Boolean,
    alsoIncludeLoggedInUserShared: Boolean,
    publicOnly: Boolean,
    publishedOnly: Boolean,
    approvedOnly: Boolean,
    moderatedOnly: Boolean)

object Visibility {
  val anonymous = Visibility(
    authenticationRequired = false,
    alsoIncludeLoggedInUserOwned = false,
    alsoIncludeLoggedInUserShared = false,
    publicOnly = true,
    publishedOnly = true,
    approvedOnly = true,
    moderatedOnly = true)

  val assetSelector = Visibility(
    authenticationRequired = true,
    alsoIncludeLoggedInUserOwned = true,
    alsoIncludeLoggedInUserShared = true,
    publicOnly = false,
    publishedOnly = false,
    approvedOnly = true,
    moderatedOnly = true)

  val personalCatalog = Visibility(
    authenticationRequired = true,
    alsoIncludeLoggedInUserOwned = false,
    alsoIncludeLoggedInUserShared = false,
    publicOnly = false,
    publishedOnly = false,
    approvedOnly = false,
    moderatedOnly = false)

  val full = Visibility(
    authenticationRequired = true,
    alsoIncludeLoggedInUserOwned = false,
    alsoIncludeLoggedInUserShared = false,
    publicOnly = false,
    publishedOnly = false,
    approvedOnly = false,
    moderatedOnly = false)
}
