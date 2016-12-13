package com.socrata.cetera.search

import org.elasticsearch.index.query.FilterBuilders._
import org.elasticsearch.index.query.{FilterBuilder, FilterBuilders}

import com.socrata.cetera.auth.User
import com.socrata.cetera.errors.UnauthorizedError
import com.socrata.cetera.handlers.UserSearchParamSet
import com.socrata.cetera.types._

object UserFilters {

  def idFilter(ids: Option[Set[String]]): Option[FilterBuilder] =
    ids.map(i => termsFilter(UserId.fieldName, i.toSeq: _*))

  def emailFilter(emails: Option[Set[String]]): Option[FilterBuilder] =
    emails.map(e => termsFilter(UserEmail.rawFieldName, e.map(_.toLowerCase).toSeq: _*))

  def screenNameFilter(screenNames: Option[Set[String]]): Option[FilterBuilder] =
    screenNames.map(s => termsFilter(UserScreenName.rawFieldName, s.map(_.toLowerCase).toSeq: _*))

  def flagFilter(flags: Option[Set[String]]): Option[FilterBuilder] =
    flags.map(r => termsFilter(UserFlag.fieldName, r.toSeq: _*))

  def roleFilter(roles: Option[Set[String]]): Option[FilterBuilder] =
    roles.map(r => termsFilter(UserRole.fieldName, r.toSeq: _*))

  def domainFilter(domainId: Option[Int]): Option[FilterBuilder] =
    domainId.map(d => termFilter(UserDomainId.fieldName, d))

  def nestedRolesFilter(roles: Option[Set[String]], domainId: Option[Int]): Option[FilterBuilder] = {
    val filters = Seq(domainFilter(domainId), roleFilter(roles)).flatten
    if (filters.isEmpty) {
      None
    } else {
      val path = "roles"
      val query = filters.foldLeft(boolFilter()) { (b, f) => b.must(f) }
      Some(nestedFilter(path, query))
    }
  }

  def authFilter(user: Option[User], domain: Option[Domain]): Option[FilterBuilder] = {
    user match {
      // if the user is searching for users on a domain, it must be the domain they are authed on
      case Some(u) if (domain.exists(d => !u.canViewUsers(d.domainId))) =>
        throw UnauthorizedError(user, s"search for users on domain ${domain.get.domainCname}")
      // if the user can view all users, no restrictions are needed (other than the one above)
      case Some(u) if (u.canViewAllUsers) => None
      // if the user can view domain users, we restrict the user search to user's authenticating domain
      case Some(u) if (u.canViewDomainUsers) => nestedRolesFilter(None, u.authenticatingDomain.map(_.domainId))
      // if the user can't view users or is not authenticated, we throw
      case _ => throw UnauthorizedError(user, "search users")
    }
  }

  def compositeFilter(
      searchParams: UserSearchParamSet,
      domain: Option[Domain],
      authorizedUser: Option[User])
  : FilterBuilder = {
    val filters = Seq(
      idFilter(searchParams.ids),
      emailFilter(searchParams.emails),
      screenNameFilter(searchParams.screenNames),
      flagFilter(searchParams.flags),
      nestedRolesFilter(searchParams.roles, domain.map(_.domainId)),
      authFilter(authorizedUser, domain)
    ).flatten
    if (filters.isEmpty) {
      FilterBuilders.matchAllFilter()
    } else {
      filters.foldLeft(boolFilter()) { (b, f) => b.must(f) }
    }
  }
}
