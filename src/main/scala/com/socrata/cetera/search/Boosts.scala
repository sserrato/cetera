package com.socrata.cetera.search

import org.elasticsearch.index.query.{BoolQueryBuilder, QueryBuilders}

import com.socrata.cetera.types.{Datatype, DatatypeFieldType, DomainFieldType}

object Boosts {
  def boostDomains(domainBoosts: Map[String, Float]): BoolQueryBuilder = {
    domainBoosts.foldLeft(QueryBuilders.boolQuery()) {
      case (q, (domain, boost)) =>
        q.should(QueryBuilders.termQuery(DomainFieldType.fieldName, domain).boost(boost))
    }
  }

  def boostDatatypes(typeBoosts: Map[Datatype, Float]): BoolQueryBuilder = {
    typeBoosts.foldLeft(QueryBuilders.boolQuery()) {
      case (q, (datatype, boost)) =>
        q.should(QueryBuilders.termQuery(DatatypeFieldType.fieldName, datatype.singular).boost(boost))
    }
  }
}
