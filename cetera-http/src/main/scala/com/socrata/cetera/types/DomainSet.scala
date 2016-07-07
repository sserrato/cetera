package com.socrata.cetera.types

// NOTE: Please note that domainBoosts are empty by default. The caller must explicitly create
// a copy of the DomainSet via the `addDomainBoosts` method with the appropriate domainBoosts
// parameter in order for those boosts to have an effect.
case class DomainSet(
    domains: Set[Domain],
    searchContext: Option[Domain],
    domainBoosts: Map[String, Float] = Map.empty) {

  def addDomainBoosts(boosts: Map[String, Float]): DomainSet = copy(domainBoosts = boosts)

  def idMap: Map[Int, Domain] = {
    val allDomains = domains ++ searchContext
    allDomains.map(d => d.domainId -> d).toMap
  }

  def idCnameMap: Map[Int, String] = {
    val allDomains = domains ++ searchContext
    allDomains.map(d => d.domainId -> d.domainCname).toMap
  }

  def cnameIdMap: Map[String, Int] = idCnameMap.map(_.swap)

  def domainIdBoosts: Map[Int, Float] = {
    val idMap = cnameIdMap
    domainBoosts.flatMap { case (cname: String, weight: Float) =>
      idMap.get(cname).map(id => id -> weight)
    }
  }

  val contextIsModerated = searchContext.exists(_.moderationEnabled)
  val allIds = domains.map(_.domainId)
  val moderationEnabledIds = domains.collect { case d: Domain if d.moderationEnabled => d.domainId }
  val moderationDisabledIds = domains.collect { case d: Domain if !d.moderationEnabled => d.domainId }
  val raDisabledIds = domains.collect { case d: Domain if !d.routingApprovalEnabled => d.domainId }
}

object DomainSet {
  def empty: DomainSet = DomainSet(Set.empty, None)
}
