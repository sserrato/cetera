package com.socrata.cetera.types

case class DomainSet(
  domains: Set[Domain],
  searchContext: Option[Domain]) {

  def idCnameMap: Map[Int, String] = {
    val allDomains = domains ++ searchContext
    allDomains.map(d => d.domainId -> d.domainCname).toMap
  }

  def cnameIdMap: Map[String, Int] = idCnameMap.map(_.swap)

  def domainIdBoosts(domainBoosts: Map[String, Float]): Map[Int, Float] = {
    val idMap = cnameIdMap
    domainBoosts.flatMap { case (cname: String, weight: Float) =>
      idMap.get(cname).map(id => id -> weight)
    }
  }

  def calculateIdsAndModRAStatuses: (Set[Int], Set[Int], Set[Int], Set[Int]) = {
    val ids = domains.map(_.domainId)
    val mod = domains.collect { case d: Domain if d.moderationEnabled => d.domainId }
    val unmod = domains.collect { case d: Domain if !d.moderationEnabled => d.domainId }
    val raOff = domains.collect { case d: Domain if !d.routingApprovalEnabled => d.domainId }
    (ids, mod, unmod, raOff)
  }
}

object DomainSet {
  def empty: DomainSet = DomainSet(Set.empty, None)
}
