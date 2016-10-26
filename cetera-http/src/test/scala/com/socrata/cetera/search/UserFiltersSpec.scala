package com.socrata.cetera.search

import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.JsonReader
import org.scalatest.{ShouldMatchers, WordSpec}

import com.socrata.cetera.TestESDomains
import com.socrata.cetera.auth.User
import com.socrata.cetera.errors.UnauthorizedError
import com.socrata.cetera.handlers.{SearchParamSet, UserSearchParamSet}
import com.socrata.cetera.types.{ApprovalStatus, Domain, DomainSet, SimpleQuery}

class UserFiltersSpec extends WordSpec with ShouldMatchers with TestESDomains {

  "the idFilter" should {
    "return the expected filter" in {
      val filter = UserFilters.idFilter(Some(Set("foo-bar"))).get
      val actual = JsonReader.fromString(filter.toString)
      val expected = j"""{"terms" : {"id" : ["foo-bar"]}}"""
      actual should be(expected)
    }
  }

  "the domainFilter" should {
    "return the expected filter" in {
      val filter = UserFilters.domainFilter(Some(1)).get
      val actual = JsonReader.fromString(filter.toString)
      val expected = j"""{"term" : {"roles.domain_id" : 1}}"""
      actual should be(expected)
    }
  }

  "the roleFilter" should {
    "return the expected filter" in {
      val filter = UserFilters.roleFilter(Some(Set("muffin", "scone"))).get
      val actual = JsonReader.fromString(filter.toString)
      val expected = j"""{"terms" : {"roles.role_name" : ["muffin", "scone"]}}"""
      actual should be(expected)
    }
  }

  "the emailFilter" should {
    "return the expected filter" in {
      val filter = UserFilters.emailFilter(Some(Set("foo@baz.bar"))).get
      val actual = JsonReader.fromString(filter.toString)
      val expected = j"""{"terms" : {"email.raw" : ["foo@baz.bar"]}}"""
      actual should be(expected)
    }
  }

  "the screenNameFilter" should {
    "return the expected filter" in {
      val filter = UserFilters.screenNameFilter(Some(Set("nombre"))).get
      val actual = JsonReader.fromString(filter.toString)
      val expected = j"""{"terms" : {"screen_name.raw" : ["nombre"]}}"""
      actual should be(expected)
    }
  }

  "the flagFilter" should {
    "return the expected filter" in {
      val filter = UserFilters.flagFilter(Some(Set("admin"))).get
      val actual = JsonReader.fromString(filter.toString)
      val expected = j"""{"terms" : {"flags" : ["admin"]}}"""
      actual should be(expected)
    }
  }

  "the authFilter" should {
    "throw an UnauthorizedError if no user is given" in  {
      an[UnauthorizedError] should be thrownBy {
        UserFilters.authFilter(None, None)
      }
    }

    "throw an UnauthorizedError if user's domain doesn't match given domain id" in  {
      val user = User("mooks", Some(domains(6)), Some("administrator"))
      an[UnauthorizedError] should be thrownBy {
        UserFilters.authFilter(Some(user), Some(domains(4)))
      }
    }

    "throw an UnauthorizedError if user hasn't a role to view users" in  {
      val user = User("mooks", Some(domains(6)), Some(""))
      an[UnauthorizedError] should be thrownBy {
        UserFilters.authFilter(Some(user), None)
      }
    }

    "return None for super admins" in {
      val user = User("mooks", flags = Some(List("admin")))
      val filter = UserFilters.authFilter(Some(user), None)
      filter should be(None)
    }

    "return None for super admins even if searching for users on a domain that isn't their authenticating domain" in {
      val user = User("mooks", Some(domains(8)), flags = Some(List("admin")))
      val filter = UserFilters.authFilter(Some(user), Some(domains(0)))
      filter should be(None)
    }

    "return None for admins who aren't snooping around other's domains" in {
      val user = User("mooks", Some(domains(1)), Some("administrator"))
      val filter = UserFilters.authFilter(Some(user), None)
      filter should be(None)
    }

    "return a nested domainId filter for non-admin roled users who aren't querying a specific domain" in {
      val user = User("mooks", Some(domains(1)), Some("i-have-a-role-really"))
      val expected = JsonReader.fromString(UserFilters.nestedRolesFilter(None, Some(1)).get.toString)
      val filter = UserFilters.authFilter(Some(user), None)
      val actual = JsonReader.fromString(filter.get.toString)
      actual should be(expected)
    }

    "return a nested domainId filter for non-admin roled users who are querying a specific domain, but it's their domain" in {
      val user = User("mooks", Some(domains(1)), Some("i-have-a-role-really"))
      val expected = JsonReader.fromString(UserFilters.nestedRolesFilter(None, Some(1)).get.toString)
      val filter = UserFilters.authFilter(Some(user), Some(domains(1)))
      val actual = JsonReader.fromString(filter.get.toString)
      actual should be(expected)
    }
  }

  "the compositeFilter" should {
    "return the expected filter" in {
      val params = UserSearchParamSet(
        emails = Some(Set("admin@gmail.com")),
        screenNames = Some(Set("Ad men")),
        flags = Some(Set("admin")),
        roles = Some(Set("admin"))
      )
      val user = User("", None, roleName = None, rights = None, flags = Some(List("admin")))
      val filter = UserFilters.compositeFilter(params, Some(domains(2)), Some(user))
      val actual = JsonReader.fromString(filter.toString)
      val expected =j"""
        {
          "bool": {
            "must": [
              { "terms": {"email.raw": ["admin@gmail.com"]}},
              { "terms": {"screen_name.raw": ["Ad men"]}},
              { "terms": {"flags": ["admin"]}},
              {
                "nested": {
                  "path": "roles",
                  "filter": {
                    "bool": {
                      "must": [
                        {"term": {"roles.domain_id": 2}},
                        {"terms": {"roles.role_name": ["admin"]}}
                      ]
                    }
                  }
                }
              }
            ]
          }
        }"""
      actual should be(expected)
    }
  }
}
