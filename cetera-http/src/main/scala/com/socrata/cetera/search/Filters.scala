package com.socrata.cetera.search

import org.elasticsearch.index.query.FilterBuilders._
import org.elasticsearch.index.query.{FilterBuilder, FilterBuilders}

import com.socrata.cetera.auth.User
import com.socrata.cetera.errors.UnauthorizedError
import com.socrata.cetera.esDocumentType
import com.socrata.cetera.handlers.{SearchParamSet, UserSearchParamSet}
import com.socrata.cetera.types._

object DocumentFilters {

  def datatypeFilter(datatypes: Set[String], aggPrefix: String = ""): FilterBuilder = {
    val validatedDatatypes = datatypes.flatMap(t => Datatype(t).map(_.singular))
    termsFilter(aggPrefix + DatatypeFieldType.fieldName, validatedDatatypes.toSeq: _*)
  }

  def userFilter(user: String, aggPrefix: String = ""): FilterBuilder =
    termFilter(aggPrefix + OwnerIdFieldType.fieldName, user)

  def sharedToFilter(user: String, aggPrefix: String = ""): FilterBuilder =
    termFilter(aggPrefix + SharedToFieldType.rawFieldName, user)

  def attributionFilter(attribution: String, aggPrefix: String = ""): FilterBuilder =
    termFilter(aggPrefix + AttributionFieldType.rawFieldName, attribution)

  def parentDatasetFilter(parentDatasetId: String, aggPrefix: String = ""): FilterBuilder =
    termFilter(aggPrefix + ParentDatasetIdFieldType.fieldName, parentDatasetId)

  def hideFromCatalogFilter(isDomainAgg: Boolean = false): FilterBuilder = {
    val prefix = if (isDomainAgg) esDocumentType + "." else ""
    notFilter(termFilter(prefix + HideFromCatalogFieldType.fieldName, true))
  }

  def idFilter(ids: Set[String]): FilterBuilder = {
    termsFilter(IdFieldType.fieldName, ids.toSeq: _*)
  }

  def domainIdFilter(domainIds: Set[Int], aggPrefix: String = ""): FilterBuilder = {
    termsFilter(aggPrefix + SocrataIdDomainIdFieldType.fieldName, domainIds.toSeq: _*)
  }

  def isApprovedByParentDomainFilter(aggPrefix: String = ""): FilterBuilder =
    termFilter(aggPrefix + "is_approved_by_parent_domain", true)

  // TODO:  should we score metadata by making this a query?
  // and if you do, remember to make the key and value both phrase matches
  def domainMetadataFilter(metadata: Option[Set[(String, String)]]): Option[FilterBuilder] =
    metadata.flatMap {
      case metadataKeyValues: Set[(String, String)] if metadataKeyValues.nonEmpty =>
        val metadataGroupedByKey = metadataKeyValues
          .groupBy { case (k, v) => k }
          .map { case (key, set) => key -> set.map(_._2) }
        val unionWithinKeys = metadataGroupedByKey.map { case (k, vs) =>
          vs.foldLeft(boolFilter()) { (b, v) =>
            b.should(
              nestedFilter(
                DomainMetadataFieldType.fieldName,
                boolFilter()
                  .must(termsFilter(DomainMetadataFieldType.Key.rawFieldName, k))
                  .must(termsFilter(DomainMetadataFieldType.Value.rawFieldName, v))
              )
            )
          }
        }

        if (metadataGroupedByKey.size == 1) {
          unionWithinKeys.headOption // no need to create an intersection for 1 key
        } else {
          val intersectAcrossKeys = unionWithinKeys.foldLeft(boolFilter()) { (b, q) => b.must(q) }
          Some(intersectAcrossKeys)
        }
      case _ => None // no filter to build from the empty set
    }

  // this filter limits results to those that are either:
  //   * from a moderated domain and default or approved
  //   * from an unmoderated domain and default if the search context is moderated
  //   * from an unmoderated domain if the search context is not moderated
  def moderationStatusFilter(
      searchContextIsModerated: Boolean,
      moderatedDomainIds: Set[Int],
      unmoderatedDomainIds: Set[Int],
      isDomainAgg: Boolean = false)
  : FilterBuilder = {
    val aggPrefix = if (isDomainAgg) esDocumentType + "." else ""
    val beDefault = termFilter(aggPrefix + IsDefaultViewFieldType.fieldName, true)
    val beAccepted = termFilter(aggPrefix + IsModerationApprovedFieldType.fieldName, true)
    val beFromUnmoderatedDomain = domainIdFilter(unmoderatedDomainIds, aggPrefix)

    val beDefaultOrApproved = boolFilter()
      .should(beDefault)
      .should(beAccepted)

    if (!searchContextIsModerated) {
      // if the context is not moderated, visible views are any from unmoderated domains or
      // default/approved views from moderated domains
      beDefaultOrApproved.should(beFromUnmoderatedDomain)
    } else if (unmoderatedDomainIds.isEmpty) {
      // if the context is moderated and every domain in question is moderated, visible views are default/approved
      beDefaultOrApproved
    } else {
      // if the context is moderated, visible views are default views from unmoderated domains and
      // default/approved views from moderated domains
      val filter = boolFilter()
      val beFromModeratedDomain = domainIdFilter(moderatedDomainIds, aggPrefix)
      filter.should(
        boolFilter()
          .must(beFromModeratedDomain)
          .must(beDefaultOrApproved)
      )
      filter.should(
        boolFilter()
          .must(beFromUnmoderatedDomain)
          .must(beDefault)
      )
      filter
    }
  }

  // this filter limits results to those that are not unapproved datalens.
  def datalensStatusFilter(isDomainAgg: Boolean = false): FilterBuilder = {
    val aggPrefix = if (isDomainAgg) esDocumentType + "." else ""
    val beADatalens = datatypeFilter(
      Set(TypeDatalenses.singular, TypeDatalensCharts.singular, TypeDatalensMaps.singular),
      aggPrefix
    )
    val beUnaccepted = notFilter(termFilter(aggPrefix + IsModerationApprovedFieldType.fieldName, true))
    notFilter(boolFilter().must(beADatalens).must(beUnaccepted))
  }

  // this filter limits results to those that are R&A approved (or from RA disabled domains when RA is not relevant)
  def routingApprovalFilter(
      searchContext: Option[Domain],
      raOffDomainIds: Set[Int],
      isDomainAgg: Boolean = false)
  : FilterBuilder = {
    val prefix = if (isDomainAgg) esDocumentType + "." else ""

    // each view must either be approved or be from a domain without R&A
    val beFromRADisabledDomain = domainIdFilter(raOffDomainIds, prefix)
    val beApprovedOrNotApplicable = boolFilter()
      .should(isApprovedByParentDomainFilter(prefix))
      .should(beFromRADisabledDomain)

    // if the search_context has R&A, views must also be approved on the search context
    val beApprovedBySearchContext = searchContext.flatMap { d =>
      if (d.routingApprovalEnabled && !isDomainAgg) {
        Some(termsFilter(ApprovingDomainIdsFieldType.fieldName, d.domainId))
      } else {
        None
      }
    }

    // the final filter insists both conditions above be true
    val filter = boolFilter().must(beApprovedOrNotApplicable)
    beApprovedBySearchContext.foreach(filter.must)
    filter
  }

  // this filter limits results to those that are public or private; public by default.
  def publicFilter(public: Boolean = true, isDomainAgg: Boolean = false): FilterBuilder = {
    val prefix = if (isDomainAgg) esDocumentType + "." else ""
    termFilter(prefix + IsPublicFieldType.fieldName, public)
  }

  // this filter limits results to those that are published or unpublished; published by default.
  def publishedFilter(published: Boolean = true, isDomainAgg: Boolean = false): FilterBuilder = {
    val prefix = if (isDomainAgg) esDocumentType + "." else ""
    termFilter(prefix + IsPublishedFieldType.fieldName, published)
  }

  // this filter limits results down to those requested by various search params
  def searchParamsFilter(searchParams: SearchParamSet, user: Option[User]): Option[FilterBuilder] = {
    val typeFilter = searchParams.datatypes.map(datatypeFilter(_))
    val ownerFilter = searchParams.user.map(userFilter(_))
    val sharingFilter = (user, searchParams.sharedTo) match {
      case (_, None) => None
      case (Some(u), Some(uid)) if (u.id == uid) => Some(sharedToFilter(uid))
      case (_, _) => throw UnauthorizedError(user, "search another user's shared files")
    }
    val attrFilter = searchParams.attribution.map(attributionFilter(_))
    val parentIdFilter = searchParams.parentDatasetId.map(parentDatasetFilter(_))
    val idsFilter = searchParams.ids.map(idFilter(_))
    val metadataFilter = searchParams.searchContext.flatMap(_ => domainMetadataFilter(searchParams.domainMetadata))

    // the params below are those that would also influence visibility. these can only serve to further
    // limit the set of views returned from what the visibilityFilters allow.
    val privacyFilter = searchParams.public.map(publicFilter(_))
    val publicationFilter = searchParams.published.map(publishedFilter(_))

    val allFilters = List(typeFilter, ownerFilter, sharingFilter, attrFilter, parentIdFilter, idsFilter,
      metadataFilter, privacyFilter, publicationFilter).flatten

    if (allFilters.isEmpty) {
      None
    } else {
      Some(allFilters.foldLeft(boolFilter()) { (b, f) => b.must(f) })
    }
  }

  // this filter limits results to those that would show at /browse , i.e. public/published/approved/unhidden
  def anonymousFilter(domainSet: DomainSet, isDomainAgg: Boolean = false)
  : FilterBuilder = {
    val bePublic = publicFilter(public = true, isDomainAgg)
    val bePublished = publishedFilter(published = true, isDomainAgg)
    val beModApproved = moderationStatusFilter(
      domainSet.searchContext.exists(_.moderationEnabled),
      domainSet.moderationEnabledIds,
      domainSet.moderationDisabledIds,
      isDomainAgg
    )
    val beDatalensApproved = datalensStatusFilter(isDomainAgg)
    val beRAApproved = routingApprovalFilter(domainSet.searchContext, domainSet.raDisabledIds, isDomainAgg)
    val beUnhidden = hideFromCatalogFilter(isDomainAgg)

    boolFilter()
      .must(bePublic)
      .must(bePublished)
      .must(beModApproved)
      .must(beDatalensApproved)
      .must(beRAApproved)
      .must(beUnhidden)
  }

  // this filter will limit results down to those owned or shared to a user
  def ownedOrSharedFilter(user: User): FilterBuilder = {
    val userId = user.id
    val ownerFilter = userFilter(userId)
    val sharedFilter = sharedToFilter(userId)
    boolFilter().should(ownerFilter).should(sharedFilter)
  }

  // this filter will limit results to only those the user is allowed to see
  def visibilityFilter(user: Option[User], domainSet: DomainSet, requireAuth: Boolean)
  : Option[FilterBuilder] = {
    (requireAuth, user) match {
      // if user is super admin, no vis filter needed
      case (true, Some(u)) if (u.isSuperAdmin) => None
      // if the user can view everything, they can only view everything on *their* domain
      // plus, of course, things they own/share and public/published/approved views from other domains
      case (true, Some(u)) if (u.authenticatingDomain.exists(d => u.canViewAllViews(d))) => {
        val personFilter = ownedOrSharedFilter(u)
        val anonFilter = anonymousFilter(domainSet)
        val fromUsersDomain = u.authenticatingDomain.map(d => domainIdFilter(Set(d.domainId)))
        val filter = boolFilter().should(personFilter).should(anonFilter)
        fromUsersDomain.foreach(filter.should(_))
        Some(filter)
      }
      // if the user isn't a superadmin nor can they view everything, they may only see
      // things they own/share and public/published/approved views
      case (true, Some(u)) => {
        val personFilter = ownedOrSharedFilter(u)
        val anonFilter = anonymousFilter(domainSet)
        Some(boolFilter().should(personFilter).should(anonFilter))
      }
      // if the user is hitting the internal endpoint without auth, we throw
      case (true, None) => throw UnauthorizedError(user, "search the internal catalog")
      // if auth isn't required, the user can only see public/published/approved views
      case (false, _) => Some(anonymousFilter(domainSet))
    }
  }

  def compositeFilter(
      domainSet: DomainSet,
      searchParams: SearchParamSet,
      user: Option[User],
      requireAuth: Boolean)
  : FilterBuilder = {
    val domainFilter = Some(domainIdFilter(domainSet.domains.map(_.domainId)))
    val searchFilter = searchParamsFilter(searchParams, user)
    val visFilters = visibilityFilter(user, domainSet, requireAuth)

    val allFilters = List(domainFilter, searchFilter, visFilters).flatten
    allFilters.foldLeft(boolFilter()) { (b, f) => b.must(f) }
  }
}

object DomainFilters {
  def idsFilter(domainIds: Set[Int]): FilterBuilder = termsFilter("domain_id", domainIds.toSeq: _*)
  def cnamesFilter(domainCnames: Set[String]): FilterBuilder =
    termsFilter(DomainCnameFieldType.rawFieldName, domainCnames.toSeq: _*)

  // two nos make a yes: this filters out items with is_customer_domain=false, while permitting true or null.
  def isNotCustomerDomainFilter: FilterBuilder = termFilter(IsCustomerDomainFieldType.fieldName, false)
  def isCustomerDomainFilter: FilterBuilder = notFilter(isNotCustomerDomainFilter)
}

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
