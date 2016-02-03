package com.socrata.cetera.search

import org.elasticsearch.index.query.{BoolQueryBuilder, QueryBuilders}

import com.socrata.cetera.types.{Datatype, DatatypeFieldType, DomainFieldType}

object Boosts {
  def boostDatatypes(
      query: BoolQueryBuilder,
      datatypeBoosts: Map[Datatype, Float])
    : BoolQueryBuilder = {

    datatypeBoosts.foldLeft(query) {
      case (q, (datatype, boost)) => q
        .should(QueryBuilders.termQuery(DatatypeFieldType.fieldName, datatype.singular)
        .boost(boost))
    }
  }

  def boostDomains(
      query: BoolQueryBuilder,
      domainBoosts: Map[String, Float])
    : BoolQueryBuilder = {

    domainBoosts.foldLeft(query) {
      case (q, (domain, boost)) => q
        .should(QueryBuilders.termQuery(DomainFieldType.rawFieldName, domain)
        .boost(boost))
    }
  }
}
