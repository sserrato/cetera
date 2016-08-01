package com.socrata.cetera.auth

class VerificationClient(coreClient: CoreClient) {

  /**
   * Get the search context domains and perform authentication/authorization on the logged-in user.
   *
   * @param domainCname the search context (customer domain)
   * @param authParams authentication params for the currently logged-in user
   * @param requestId a somewhat unique identifier that helps string requests together across services
   * @param authorized a function specifying whether the User returned by core is authorized
   * @return (user if authorized else None, client cookies to set)
   */
  def fetchUserAuthorization(
      domainCname: Option[String],
      authParams: AuthParams,
      requestId: Option[String],
      authorized: User => Boolean)
      : (Option[User], Seq[String]) = {
    val (user, setCookies) = coreClient.optionallyAuthenticateUser(domainCname, authParams, requestId)
    (user.filter(authorized), setCookies)
  }
}
