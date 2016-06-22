package com.socrata.cetera.types

import org.scalatest._

import com.socrata.cetera.{TestESClient, TestESData}

class DomainSetSpec extends WordSpec with ShouldMatchers with TestESData {

  val client = new TestESClient(testSuiteName)

  "the idCnameMap" should {
    "return an empty map if no domains are given" in {
      val domainSet = DomainSet(Set.empty, None)
      val actualMap = domainSet.idCnameMap
      actualMap should be(empty)
    }

    "return a map with one entry if only a search context is given" in {
      val domainSet = DomainSet(Set.empty, Some(domains(0)))
      val actualMap = domainSet.idCnameMap
      val expectedMap = Map(0 -> "petercetera.net")
      actualMap should be(expectedMap)
    }

    "return a map with one entry if only a single domain is given" in {
      val domainSet = DomainSet(Set(domains(2)), None)
      val actualMap = domainSet.idCnameMap
      val expectedMap = Map(2 -> "blue.org")
      actualMap should be(expectedMap)
    }

    "return a map with all the entries if both domains and a search context is given" in {
      val domainSet = DomainSet(Set(domains(2), domains(0)), Some(domains(0)))
      val actualMap = domainSet.idCnameMap
      val expectedMap = Map(0 -> "petercetera.net", 2 -> "blue.org")
      actualMap should be(expectedMap)
    }
  }

  "the cnameIdMap" should {
    "return an empty map if no domains are given" in {
      val domainSet = DomainSet(Set.empty, None)
      val actualMap = domainSet.cnameIdMap
      actualMap should be(empty)
    }

    "return a map with one entry if only a search context is given" in {
      val domainSet = DomainSet(Set.empty, Some(domains(0)))
      val actualMap = domainSet.cnameIdMap
      val expectedMap = Map("petercetera.net" -> 0)
      actualMap should be(expectedMap)
    }

    "return a map with one entry if only a single domain is given" in {
      val domainSet = DomainSet(Set(domains(2)), None)
      val actualMap = domainSet.cnameIdMap
      val expectedMap = Map("blue.org" -> 2)
      actualMap should be(expectedMap)
    }

    "return a map with all the entries if both domains and a search context is given" in {
      val domainSet = DomainSet(Set(domains(2), domains(0)), Some(domains(0)))
      val actualMap = domainSet.cnameIdMap
      val expectedMap = Map("petercetera.net" -> 0, "blue.org" -> 2)
      actualMap should be(expectedMap)
    }
  }

  "the domainIdBoosts method" should {
    "convert domainBoosts if the domains in question are within the DomainSet" in {
      val domainSet = DomainSet(Set(domains(3), domains(4)), Some(domains(4)))
      val boosts = Map[String, Float]("annabelle.island.net" -> 0.42f, "dylan.demo.socrata.com" -> 0.58f)
      val actualBoosts = domainSet.domainIdBoosts(boosts)
      val expectedBoosts = Map(3 -> 0.42f, 4 -> 0.58f)
      actualBoosts should be(expectedBoosts)
    }

    "drop out cnames if the domains in question are not within the DomainSet" in {
      val domainSet = DomainSet(Set(domains(3), domains(4)), Some(domains(4)))
      val boosts = Map[String, Float]("annabelle.island.net" -> 0.33f, "blue.org" -> 0.67f)
      val actualBoosts = domainSet.domainIdBoosts(boosts)
      val expectedBoosts = Map(3 -> 0.33f)
      actualBoosts should be(expectedBoosts)
    }
  }

  "the calculateIdsAndModRAStatuses method" should {
    "bucket domains appropriately" in {
      val domainSet = DomainSet(domains.toSet, Some(domains(1)))
      val actualGoods = domainSet.calculateIdsAndModRAStatuses
      val expectedIds = domains.map(_.domainId).toSet
      val expectedMod = Set(1, 3, 6, 7, 8)
      val expectedNotMod = Set(0, 2, 4, 5)
      val expectedRA = Set(0, 1, 5, 7)
      actualGoods._1 should be(expectedIds)
      actualGoods._2 should be(expectedMod)
      actualGoods._3 should be(expectedNotMod)
      actualGoods._4 should be(expectedRA)
    }
  }
}
