package com.socrata.cetera.search

import org.elasticsearch.index.query.FilterBuilders._
import org.elasticsearch.index.query.{BoolFilterBuilder, FilterBuilder, FilterBuilders, MatchAllFilterBuilder}

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

  def provenanceFilter(provenance: String, aggPrefix: String = ""): FilterBuilder =
    termFilter(aggPrefix + ProvenanceFieldType.rawFieldName, provenance)

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

  def approvedByParentDomainFilter(aggPrefix: String = ""): FilterBuilder =
    termFilter(aggPrefix + ApprovedByParentFieldType.fieldName, true)

  def derivedFilter(derived: Boolean = true, aggPrefix: String = ""): FilterBuilder =
    termFilter(aggPrefix + IsDefaultViewFieldType.fieldName, !derived)

  def explicitlyHiddenFilter(hidden: Boolean = true, aggPrefix: String = ""): FilterBuilder =
    termFilter(aggPrefix + HideFromCatalogFieldType.fieldName, hidden)

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

  // this filter, if the context is moderated, will limit results to
  // views from moderated domains + default views from unmoderated sites
  def vmSearchContextFilter(domainSet: DomainSet): Option[FilterBuilder] =
    domainSet.searchContext.collect { case c: Domain if c.moderationEnabled =>
      val beDefault = termFilter(IsDefaultViewFieldType.fieldName, true)
      val beFromModeratedDomain = domainIdFilter(domainSet.moderationEnabledIds)
      boolFilter().should(beDefault).should(beFromModeratedDomain)
    }

  // this filter, if the context has RA enabled, will limit results to only those
  // having been through or are presently in the context's RA queue
  def raSearchContextFilter(domainSet: DomainSet): Option[FilterBuilder] =
    domainSet.searchContext.collect { case c: Domain if c.routingApprovalEnabled =>
      val beApprovedByContext = termsFilter(ApprovingDomainIdsFieldType.fieldName, c.domainId)
      // TODO: define these and add them in when we know about RA rejected/pending
      // val beRejectedByContext = ???
      // val bePendingWithinContextsQueue = ???
      boolFilter().should(beApprovedByContext) // .should(beRejectedByContext).should(bePendingWithinContextsQueue)
    }

  // this filter limits results based on the processes in place on the search context
  //  - if view moderation is enabled, all derived views from unmoderated federated domains are removed
  //  - if R&A is enabled, all views whose self or parent has not been through the context's RA queue are removed
  def searchContextFilter(domainSet: DomainSet, isDomainAgg: Boolean = false): Option[FilterBuilder] = {
    val vmFilter = vmSearchContextFilter(domainSet)
    val raFilter = raSearchContextFilter(domainSet)
    List(vmFilter, raFilter).flatten match {
      case Nil => None
      case filters: Seq[FilterBuilder] => Some(filters.foldLeft(boolFilter()) { (b, f) => b.must(f) })
    }
  }

  // this filter limits results down to views that are both
  //  - from the set of requested domains
  //  - should be included according to the search context and our funky federation rules
  def domainSetFilter(domainSet: DomainSet): FilterBuilder = {
    val domainsFilter = domainIdFilter(domainSet.domains.map(_.domainId))
    val contextFilter = searchContextFilter(domainSet)
    val filters = List(domainsFilter) ++ contextFilter
    filters.foldLeft(boolFilter()) { (b, f) => b.must(f) }
  }

  // this filter limits results to those with the given status
  def moderationStatusFilter(status: ApprovalStatus, domainSet: DomainSet, isDomainAgg: Boolean = false)
  : FilterBuilder = {
    val aggPrefix = if (isDomainAgg) esDocumentType + "." else ""

    status match {
      case ApprovalStatus.approved =>
        // to be approved a view must either be
        //   * a default/approved view from a moderated domain
        //   * any view from an unmoderated domain (as they are approved by default)
        // NOTE: funkiness with federation is handled by the searchContext filter.
        val beDefault = termFilter(aggPrefix + IsDefaultViewFieldType.fieldName, true)
        val beApproved = termFilter(aggPrefix + ModerationStatusFieldType.fieldName, ApprovalStatus.approved.status)
        val beFromUnmoderatedDomain = domainIdFilter(domainSet.moderationDisabledIds, aggPrefix)
        boolFilter().should(beDefault).should(beApproved).should(beFromUnmoderatedDomain)
      case _ =>
        // to be rejected/pending, a view must be from a moderated domain with the given status
        val beFromModeratedDomain = domainIdFilter(domainSet.moderationEnabledIds, aggPrefix)
        val haveGivenStatus = termFilter(aggPrefix + ModerationStatusFieldType.fieldName, status.status)
        boolFilter().must(beFromModeratedDomain).must(haveGivenStatus)
    }
  }

  def datalensStatusFilter(status: ApprovalStatus, isDomainAgg: Boolean = false): FilterBuilder = {
    val aggPrefix = if (isDomainAgg) esDocumentType + "." else ""
    val beADatalens = datatypeFilter(TypeDatalenses.allVarieties, aggPrefix)
    status match {
      case ApprovalStatus.approved =>
        // limit results to those that are not unapproved datalens
        val beUnapproved =
          notFilter(termFilter(aggPrefix + ModerationStatusFieldType.fieldName, ApprovalStatus.approved.status))
        notFilter(boolFilter().must(beADatalens).must(beUnapproved))
      case _ =>
        // limit results to those with the given status
        val haveGivenStatus = termFilter(aggPrefix + ModerationStatusFieldType.fieldName, status.status)
        boolFilter().must(beADatalens).must(haveGivenStatus)
    }
  }

  // this filter limits results to those that are R&A approved (or from RA disabled domains when RA is not relevant)
  def routingApprovalFilter(domainSet: DomainSet, isDomainAgg: Boolean = false): FilterBuilder = {
    val prefix = if (isDomainAgg) esDocumentType + "." else ""

    // each view must either be approved or be from a domain without R&A
    val beFromRADisabledDomain = domainIdFilter(domainSet.raDisabledIds, prefix)
    val beApprovedOrNotApplicable = boolFilter()
      .should(approvedByParentDomainFilter(prefix))
      .should(beFromRADisabledDomain)

    // if the search_context has R&A, views must also be approved on the search context
    val beApprovedBySearchContext = domainSet.searchContext.collect {
      case d: Domain if d.routingApprovalEnabled && !isDomainAgg =>
        termsFilter(ApprovingDomainIdsFieldType.fieldName, d.domainId)
    }

    // the final filter insists both conditions above be true
    val filter = boolFilter().must(beApprovedOrNotApplicable)
    beApprovedBySearchContext.foreach(filter.must)
    filter
  }

  // this filter limits results to those with the given R&A status
  def raStatusFilter(status: ApprovalStatus, domainSet: DomainSet, isDomainAgg: Boolean = false): FilterBuilder = {
    status match {
      case ApprovalStatus.approved => routingApprovalFilter(domainSet, isDomainAgg)
      // TODO: finish when have all the RA info
      case _ => domainIdFilter(Set.empty) // this is just a hack to not match anything yet
    }
  }

  // this filter limits results to those with a given approval status
  // approved views must pass all 3 processes (viewModertion, R&A and Datalens approval)
  // rejected/pending views need be rejected/pending by 1 or more processes
  def approvalStatusFilter(
      status: ApprovalStatus,
      domainSet: DomainSet,
      isDomainAgg: Boolean = false)
  : FilterBuilder = {
    val haveGivenVMStatus = moderationStatusFilter(status, domainSet, isDomainAgg)
    val haveGivenDLStatus = datalensStatusFilter(status, isDomainAgg)
    val haveGivenRAStatus = raStatusFilter(status, domainSet, isDomainAgg)
    status match {
      case ApprovalStatus.approved =>
        // if we are looking for approvals, a view must be approved according to all 3 processes
        boolFilter().must(haveGivenVMStatus).must(haveGivenDLStatus).must(haveGivenRAStatus)
      case _ =>
        // otherwise, a view can fail due to any of the 3 processes
        boolFilter().should(haveGivenVMStatus).should(haveGivenDLStatus).should(haveGivenRAStatus)
    }
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
  def searchParamsFilter(searchParams: SearchParamSet, user: Option[User], domainSet: DomainSet)
  : Option[FilterBuilder] = {
    val typeFilter = searchParams.datatypes.map(datatypeFilter(_))
    val ownerFilter = searchParams.user.map(userFilter(_))
    val sharingFilter = (user, searchParams.sharedTo) match {
      case (_, None) => None
      case (Some(u), Some(uid)) if (u.id == uid) => Some(sharedToFilter(uid))
      case (_, _) => throw UnauthorizedError(user, "search another user's shared files")
    }
    val attrFilter = searchParams.attribution.map(attributionFilter(_))
    val provFilter = searchParams.provenance.map(provenanceFilter(_))
    val parentIdFilter = searchParams.parentDatasetId.map(parentDatasetFilter(_))
    val idsFilter = searchParams.ids.map(idFilter(_))
    val metadataFilter = searchParams.searchContext.flatMap(_ => domainMetadataFilter(searchParams.domainMetadata))
    val derivationFilter = searchParams.derived.map(derivedFilter(_))

    // the params below are those that would also influence visibility. these can only serve to further
    // limit the set of views returned from what the visibilityFilters allow.
    val privacyFilter = searchParams.public.map(publicFilter(_))
    val publicationFilter = searchParams.published.map(publishedFilter(_))
    val hiddenFilter = searchParams.explicitlyHidden.map(explicitlyHiddenFilter(_))
    val approvalFilter = searchParams.approvalStatus.map(approvalStatusFilter(_, domainSet))

    List(typeFilter, ownerFilter, sharingFilter, attrFilter, provFilter, parentIdFilter, idsFilter, metadataFilter,
      derivationFilter, privacyFilter, publicationFilter, hiddenFilter, approvalFilter).flatten match {
      case Nil => None
      case filters: Seq[FilterBuilder] => Some(filters.foldLeft(boolFilter()) { (b, f) => b.must(f) })
    }
  }

  // this filter limits results to those that would show at /browse , i.e. public/published/approved/unhidden
  def anonymousFilter(domainSet: DomainSet, isDomainAgg: Boolean = false)
  : FilterBuilder = {
    val bePublic = publicFilter(public = true, isDomainAgg)
    val bePublished = publishedFilter(published = true, isDomainAgg)
    val beApproved = approvalStatusFilter(ApprovalStatus.approved, domainSet, isDomainAgg)
    val beUnhidden = hideFromCatalogFilter(isDomainAgg)

    boolFilter()
      .must(bePublic)
      .must(bePublished)
      .must(beApproved)
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
    val domainFilter = Some(domainSetFilter(domainSet))
    val searchFilter = searchParamsFilter(searchParams, user, domainSet)
    val visFilters = visibilityFilter(user, domainSet, requireAuth)

    val allFilters = List(domainFilter, searchFilter, visFilters).flatten
    allFilters.foldLeft(boolFilter()) { (b, f) => b.must(f) }
  }
}
