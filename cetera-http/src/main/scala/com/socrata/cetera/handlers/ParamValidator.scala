package com.socrata.cetera.handlers

import com.socrata.cetera.search.Visibility

case class ParamValidator(
    searchParams: SearchParamSet,
    loggedInUserId: Option[String],
    visibility: Visibility) {

  def userParamsAuthorized: Boolean =
  // A user can never filter items by what's been shared to a different user
  // If going through the personal_catalog endpoint, a user can't view others' personal assets
  // Can view another user's public assets (ie filter by "for_user") via regular endpoint
    !searchParams.sharedTo.exists(s => s != loggedInUserId.getOrElse(None)) &&
      !(visibility == Visibility.personalCatalog && searchParams.user.nonEmpty && searchParams.user != loggedInUserId)
}

