package com.socrata.cetera.search

import org.elasticsearch.index.query.FilterBuilder
import org.elasticsearch.index.query.FilterBuilders._

import com.socrata.cetera.types.{DomainCnameFieldType, IsCustomerDomainFieldType}

object DomainFilters {
  def idsFilter(domainIds: Set[Int]): FilterBuilder = termsFilter("domain_id", domainIds.toSeq: _*)
  def cnamesFilter(domainCnames: Set[String]): FilterBuilder =
    termsFilter(DomainCnameFieldType.rawFieldName, domainCnames.toSeq: _*)

  // two nos make a yes: this filters out items with is_customer_domain=false, while permitting true or null.
  def isNotCustomerDomainFilter: FilterBuilder = termFilter(IsCustomerDomainFieldType.fieldName, false)
  def isCustomerDomainFilter: FilterBuilder = notFilter(isNotCustomerDomainFilter)
}
