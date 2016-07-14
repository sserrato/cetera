package com.socrata.cetera.auth

class VerificationClient(coreClient: CoreClient) {

  /** Gets the search context domains and performs authentication/authorization on the logged-in user.
    *
    * @param domainCname the search context (customer domain)
    * @param cookie the currently logged-in user's core cookie
    * @param requestId a somewhat unique identifier that helps string requests together across services
    * @param authorized a function specifying whether the User returned by core is authorized
    * @return (user if authorized else None, client cookies to set)
    */
  def fetchUserAuthorization(
    domainCname: Option[String],
    cookie: Option[String],
    requestId: Option[String],
    authorized: User => Boolean)
  : (Option[User], Seq[String]) =
    (domainCname, cookie) match {
      case (Some(extendedHost), Some(nomNom)) =>
        // Hi, my name is Werner Brandes. My voice is my passport, verify me.
        val (authUser, setCookies) = coreClient.fetchUserByCookie(extendedHost, nomNom, requestId)
        (authUser.collect { case user: User if authorized(user) => user}, setCookies)
      case (_,_) => (None, Seq.empty[String])
    }
}
