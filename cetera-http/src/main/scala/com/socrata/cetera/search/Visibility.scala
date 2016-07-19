package com.socrata.cetera.search

case class Visibility(
    authenticationRequired: Boolean,
    alsoIncludeLoggedInUserOwned: Boolean,
    alsoIncludeLoggedInUserShared: Boolean,
    loggedInUserOwnedOnly: Boolean,
    loggedInUserSharedOnly: Boolean,
    publicOnly: Boolean,
    publishedOnly: Boolean,
    approvedOnly: Boolean,
    moderatedOnly: Boolean)

object Visibility {
  val anonymous = Visibility(
    authenticationRequired = false,
    alsoIncludeLoggedInUserOwned = false,
    alsoIncludeLoggedInUserShared = false,
    loggedInUserOwnedOnly = false,
    loggedInUserSharedOnly = false,
    publicOnly = true,
    publishedOnly = true,
    approvedOnly = true,
    moderatedOnly = true)

  val assetSelector = Visibility(
    authenticationRequired = true,
    alsoIncludeLoggedInUserOwned = true,
    alsoIncludeLoggedInUserShared = true,
    loggedInUserOwnedOnly = false,
    loggedInUserSharedOnly = false,
    publicOnly = false,
    publishedOnly = false,
    approvedOnly = true,
    moderatedOnly = true)

  val personallyOwned = Visibility(
    authenticationRequired = true,
    alsoIncludeLoggedInUserOwned = false,
    alsoIncludeLoggedInUserShared = false,
    loggedInUserOwnedOnly = true,
    loggedInUserSharedOnly = false,
    publicOnly = false,
    publishedOnly = false,
    approvedOnly = false,
    moderatedOnly = false)

  val personallyShared = Visibility(
    authenticationRequired = true,
    alsoIncludeLoggedInUserOwned = false,
    alsoIncludeLoggedInUserShared = false,
    loggedInUserOwnedOnly = false,
    loggedInUserSharedOnly = true,
    publicOnly = false,
    publishedOnly = false,
    approvedOnly = false,
    moderatedOnly = false)

  val full = Visibility(
    authenticationRequired = true,
    alsoIncludeLoggedInUserOwned = false,
    alsoIncludeLoggedInUserShared = false,
    loggedInUserOwnedOnly = false,
    loggedInUserSharedOnly = false,
    publicOnly = false,
    publishedOnly = false,
    approvedOnly = false,
    moderatedOnly = false)
}
