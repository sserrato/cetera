package com.socrata.cetera.search

import org.elasticsearch.index.query.{FilterBuilder, FilterBuilders}
import org.elasticsearch.index.query.FilterBuilders._

import com.socrata.cetera.auth.User
import com.socrata.cetera.errors.UnauthorizedError
import com.socrata.cetera.handlers.UserSearchParamSet
import com.socrata.cetera.types._

object UserFilters {

  def idFilter(ids: Option[Set[String]]): Option[FilterBuilder] =
    ids.map(i => termsFilter(UserId.fieldName, i.toSeq: _*))

  def emailFilter(emails: Option[Set[String]]): Option[FilterBuilder] =
    emails.map(e => termsFilter(UserEmail.rawFieldName, e.toSeq: _*))

  def screenNameFilter(screenNames: Option[Set[String]]): Option[FilterBuilder] =
    screenNames.map(s => termsFilter(UserScreenName.rawFieldName, s.toSeq: _*))

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

  def visibilityFilter(user: Option[User], domainId: Option[Int]): Option[FilterBuilder] = {
    (user, domainId) match {
      // if user is super admin, no vis filter needed
      case (Some(u), _) if (u.isSuperAdmin) => None
      // if the user can view users and isn't enquiring about a domian, they can view all users
      case (Some(u), None) if (u.canViewUsers) => None
      // if the user can view users and is enquiring about a domian, it must be their domain
      case (Some(u), Some(id)) if (u.canViewUsers && u.authenticatingDomain.exists(_.domainId == id)) => None
      // if the user isn't a superadmin or can't view users or is nosing for users on other domains, we throw
      case (_, _) => throw UnauthorizedError(user, "search users")
    }
  }

  def compositeFilter(
      searchParams: UserSearchParamSet,
      domainId: Option[Int],
      authorizedUser: Option[User])
  : FilterBuilder = {
    val filters = Seq(
      idFilter(searchParams.ids),
      emailFilter(searchParams.emails),
      screenNameFilter(searchParams.screenNames),
      flagFilter(searchParams.flags),
      nestedRolesFilter(searchParams.roles, domainId),
      visibilityFilter(authorizedUser, domainId)
    ).flatten
    if (filters.isEmpty) {
      FilterBuilders.matchAllFilter()
    } else {
      filters.foldLeft(boolFilter()) { (b, f) => b.must(f) }
    }
  }
}
