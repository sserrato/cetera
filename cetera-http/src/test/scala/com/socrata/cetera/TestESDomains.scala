package com.socrata.cetera

import scala.io.Source

import com.socrata.cetera.types.Domain

trait TestESDomains {

  val domains = {
    val domainTSV = Source.fromInputStream(getClass.getResourceAsStream("/domains.tsv"))
    val iter = domainTSV.getLines().map(_.split("\t"))
    iter.drop(1) // drop the header columns
    iter.map { tsvLine =>
      Domain(
        domainId = tsvLine(0).toInt,
        domainCname = tsvLine(1),
        siteTitle = Option(tsvLine(2)).filter(_.nonEmpty),
        organization = Option(tsvLine(3)).filter(_.nonEmpty),
        isCustomerDomain = tsvLine(4).toBoolean,
        moderationEnabled = tsvLine(5).toBoolean,
        routingApprovalEnabled = tsvLine(6).toBoolean,
        lockedDown = tsvLine(7).toBoolean,
        apiLockedDown = tsvLine(8).toBoolean,
        unmigratedNbeEnabled = tsvLine(9).toBoolean
      )
    }.toSeq
  }
}
