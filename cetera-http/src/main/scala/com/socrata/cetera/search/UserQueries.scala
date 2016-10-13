package com.socrata.cetera.search

import org.apache.lucene.queryparser.flexible.standard.QueryParserUtil
import org.elasticsearch.index.query.{FilteredQueryBuilder, QueryBuilder, QueryBuilders}

import com.socrata.cetera.auth.User
import com.socrata.cetera.handlers.UserSearchParamSet
import com.socrata.cetera.search.UserFilters.compositeFilter
import com.socrata.cetera.types.{UserEmail, UserScreenName}

object UserQueries {
  def emailNameMatchQuery(query: Option[String]): QueryBuilder =
    query match {
      case None => QueryBuilders.matchAllQuery()
      case Some(q) =>
        val sanitizedQ = QueryParserUtil.escape(q)
        QueryBuilders
          .queryStringQuery(sanitizedQ)
          .field(UserScreenName.fieldName)
          .field(UserScreenName.rawFieldName)
          .field(UserEmail.fieldName)
          .field(UserEmail.rawFieldName)
          .autoGeneratePhraseQueries(true)
    }

  def userQuery(
      searchParams: UserSearchParamSet,
      domainId: Option[Int],
      authorizedUser: Option[User])
  : FilteredQueryBuilder = {

    val emailOrNameQuery = emailNameMatchQuery(searchParams.query)
    val userFilter = compositeFilter(searchParams, domainId, authorizedUser)
    QueryBuilders.filteredQuery(emailOrNameQuery, userFilter)
  }
}
