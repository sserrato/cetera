package com.socrata.cetera.search

import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.JsonReader
import org.scalatest.{ShouldMatchers, WordSpec}

import com.socrata.cetera.TestESDomains
import com.socrata.cetera.auth.User
import com.socrata.cetera.errors.UnauthorizedError
import com.socrata.cetera.handlers.{SearchParamSet, UserSearchParamSet}
import com.socrata.cetera.types.{Domain, DomainSet, SimpleQuery}

class FiltersSpec extends WordSpec with ShouldMatchers with TestESDomains {

  "DocumentFilters: datatypeFilter" should {
    "return the expected filter if one datatype is given" in {
      val datatypeFilter = DocumentFilters.datatypeFilter(Set("datasets"))
      val expected = j"""{ "terms": { "datatype": [ "dataset" ] } }"""
      val actual = JsonReader.fromString(datatypeFilter.toString)
      actual should be(expected)
    }

    "return the expected filter if multiple datatypes are given" in {
      val datatypeFilter = DocumentFilters.datatypeFilter(Set("datasets", "datalens"))
      val expected = j"""{ "terms": { "datatype": [ "dataset", "datalens" ] } }"""
      val actual = JsonReader.fromString(datatypeFilter.toString)
      actual should be(expected)
    }

    "return the expected filter if a prefix is used" in {
      val datatypeFilter = DocumentFilters.datatypeFilter(Set("pulses"), "new_")
      val expected = j"""{ "terms": { "new_datatype": [ "pulse" ] } }"""
      val actual = JsonReader.fromString(datatypeFilter.toString)
      actual should be(expected)
    }
  }

  "DocumentFilters: userFilter" should {

    "return the expected filter if a user is given" in {
      val userFilter = DocumentFilters.userFilter("wonder-woman")
      val expected = j"""{ "term": { "owner_id": "wonder-woman" } }"""
      val actual = JsonReader.fromString(userFilter.toString)
      actual should be(expected)
    }

    "return the expected filter if a prefix is used" in {
      val userFilter = DocumentFilters.userFilter("wonder-woman", "document.")
      val expected = j"""{ "term": { "document.owner_id": "wonder-woman" } }"""
      val actual = JsonReader.fromString(userFilter.toString)
      actual should be(expected)
    }
  }

  "DocumentFilters: sharedToFilter" should {
    "return the expected filter if sharedTo is given" in {
      val sharedToFilter = DocumentFilters.sharedToFilter("wonder-woman")
      val expected = j"""{ "term": { "shared_to": "wonder-woman" } }"""
      val actual = JsonReader.fromString(sharedToFilter.toString)
      actual should be(expected)
    }

    "return the expected filter if a prefix is used" in {
      val sharedToFilter = DocumentFilters.sharedToFilter("wonder-woman", "document.")
      val expected = j"""{ "term": { "document.shared_to": "wonder-woman" } }"""
      val actual = JsonReader.fromString(sharedToFilter.toString)
      actual should be(expected)
    }
  }

  "DocumentFilters: attributionFilter" should {
    "return the expected filter if some attribution is given" in {
      val attrFilter = DocumentFilters.attributionFilter("wonder-woman")
      val expected = j"""{ "term": { "attribution.raw": "wonder-woman" } }"""
      val actual = JsonReader.fromString(attrFilter.toString)
      actual should be(expected)
    }

    "return the expected filter if a prefix is used" in {
      val attrFilter = DocumentFilters.attributionFilter("wonder-woman", "document.")
      val expected = j"""{ "term": { "document.attribution.raw": "wonder-woman" } }"""
      val actual = JsonReader.fromString(attrFilter.toString)
      actual should be(expected)
    }
  }

  "DocumentFilters: parentDatasetFilter" should {
    "return the expected filter if some id is given" in {
      val pdFilter = DocumentFilters.parentDatasetFilter("wonder-woman")
      val expected = j"""{ "term": { "socrata_id.parent_dataset_id": "wonder-woman" } }"""
      val actual = JsonReader.fromString(pdFilter.toString)
      actual should be(expected)
    }

    "return the expected filter if a prefix is used" in {
      val pdFilter = DocumentFilters.parentDatasetFilter("wonder-woman", "document.")
      val expected = j"""{ "term": { "document.socrata_id.parent_dataset_id": "wonder-woman" } }"""
      val actual = JsonReader.fromString(pdFilter.toString)
      actual should be(expected)
    }
  }

  "DocumentFilters: IdFilter" should {
    "return None if the set of ids is empty" in {
      val idFilter = DocumentFilters.idFilter(Set.empty)
      val expected = j"""{ "terms": { "_id": [] } }"""
      val actual = JsonReader.fromString(idFilter.toString)
      actual should be(expected)
    }

    "return the expected filter if one id is given" in {
      val idFilter = DocumentFilters.idFilter(Set("id-one"))
      val expected = j"""{ "terms": { "_id": [ "id-one" ] } }"""
      val actual = JsonReader.fromString(idFilter.toString)
      actual should be(expected)
    }

    "return the expected filter if multiple ids are given" in {
      val idFilter = DocumentFilters.idFilter(Set("id-one", "id-two"))
      val expected = j"""{ "terms": { "_id": [ "id-one", "id-two" ] } }"""
      val actual = JsonReader.fromString(idFilter.toString)
      actual should be(expected)
    }
  }

  "DocumentFilters: domainIdsFilter" should {
    "return None if the set of ids is empty" in {
      val domainIdFilter = DocumentFilters.domainIdFilter(Set.empty)
      val expected = j"""{ "terms": { "socrata_id.domain_id": [ ] } }"""
      val actual = JsonReader.fromString(domainIdFilter.toString)
      actual should be(expected)
    }

    "return the expected filter if one id is given" in {
      val domainIdFilter = DocumentFilters.domainIdFilter(Set(1))
      val expected = j"""{ "terms": { "socrata_id.domain_id": [ 1 ] } }"""
      val actual = JsonReader.fromString(domainIdFilter.toString)
      actual should be(expected)
    }

    "return the expected filter if multiple ids are given" in {
      val domainIdFilter = DocumentFilters.domainIdFilter(Set(1, 42))
      val expected = j"""{ "terms": { "socrata_id.domain_id": [ 1, 42 ] } }"""
      val actual = JsonReader.fromString(domainIdFilter.toString)
      actual should be(expected)
    }

    "return the expected filter if a prefix is used" in {
      val domainIdFilter = DocumentFilters.domainIdFilter(Set(1, 42), "demo.")
      val expected = j"""{ "terms": { "demo.socrata_id.domain_id": [ 1, 42 ] } }"""
      val actual = JsonReader.fromString(domainIdFilter.toString)
      actual should be(expected)
    }
  }

  "DocumentFilters: isApprovedByParentDomainFilter" should {
    "return the expected filter when no prefix is given" in {
      val approvalFilter = DocumentFilters.isApprovedByParentDomainFilter()
      val expected = j"""{ "term": { "is_approved_by_parent_domain": true } }"""
      val actual = JsonReader.fromString(approvalFilter.toString)
      actual should be(expected)
    }

    "return the expected filter when some prefix is given" in {
      val approvalFilter = DocumentFilters.isApprovedByParentDomainFilter("child_")
      val expected = j"""{ "term": { "child_is_approved_by_parent_domain": true } }"""
      val actual = JsonReader.fromString(approvalFilter.toString)
      actual should be(expected)
    }
  }

  "DocumentFilters: domainMetadataFilter" should {
    "return the expected filter when no metadata is given" in {
      val filter = DocumentFilters.domainMetadataFilter(None)
      filter should be(None)
    }

    "return the expected filter when an empty set of metadata is given" in {
      val filter = DocumentFilters.domainMetadataFilter(Some(Set.empty[(String, String)]))
      filter should be(None)
    }

    "return the expected filter when a metadata query lists a single key, single value pair" in {
      val filter = DocumentFilters.domainMetadataFilter(Some(Set(("org", "ny"))))
      val expected =
        j"""{"bool": {"should": {"nested": {"filter": {"bool": {"must": [
            {"terms": {"customer_metadata_flattened.key.raw": [ "org" ] }},
            {"terms": {"customer_metadata_flattened.value.raw": [ "ny" ] }}
          ]}}, "path": "customer_metadata_flattened" }}}
      }"""
      val actual = JsonReader.fromString(filter.get.toString)
      actual should be(expected)
    }

    "return the expected filter when a metadata query lists a single key but multiple values" in {
      val filter = DocumentFilters.domainMetadataFilter(Some(Set(("org", "ny"), ("org", "nj"))))
      val expected =
        j"""{"bool": {"should": [
            {"nested": {"filter": {"bool": {"must": [
              {"terms": {"customer_metadata_flattened.key.raw": [ "org" ] }},
              {"terms": {"customer_metadata_flattened.value.raw": [ "ny" ] }}
            ]}}, "path": "customer_metadata_flattened" }},
            {"nested": {"filter": {"bool": {"must": [
              {"terms": {"customer_metadata_flattened.key.raw": [ "org" ] }},
              {"terms": {"customer_metadata_flattened.value.raw": [ "nj" ] }}
            ]}}, "path": "customer_metadata_flattened" }}]}
      }"""
      val actual = JsonReader.fromString(filter.get.toString)
      actual should be(expected)
    }

    "return the expected filter when a metadata query lists multiple sets of single key/single value pairs" in {
      val filter = DocumentFilters.domainMetadataFilter(Some(Set(("org", "chicago"), ("owner", "john"))))
      val expected =
        j"""{"bool": {"must": [
               {"bool": {"should": {"nested": {"filter": {"bool": {"must": [
                 {"terms": {"customer_metadata_flattened.key.raw": [ "org" ] }},
                 {"terms": {"customer_metadata_flattened.value.raw": [ "chicago" ] }}
               ]}}, "path": "customer_metadata_flattened" }}}},
               {"bool": {"should": {"nested": {"filter": {"bool": {"must": [
                 {"terms": {"customer_metadata_flattened.key.raw": [ "owner" ] }},
                 {"terms": {"customer_metadata_flattened.value.raw": [ "john" ] }}
               ]}}, "path": "customer_metadata_flattened" }}}}
             ]}
      }"""
      val actual = JsonReader.fromString(filter.get.toString)
      actual should be(expected)
    }

    "return the expected filter when a metadata query lists multiple sets of keys with multiple values" in {
      val filter = DocumentFilters.domainMetadataFilter(Some(Set(("org", "chicago"), ("owner", "john"), ("org", "ny"))))
      val expected =
        j"""{"bool": {"must": [
               {"bool": {"should": [
                  {"nested": {"filter": {"bool": {"must": [
                    {"terms": {"customer_metadata_flattened.key.raw": [ "org" ] }},
                    {"terms": {"customer_metadata_flattened.value.raw": [ "chicago" ] }}
                  ]}}, "path": "customer_metadata_flattened" }},
                  {"nested": {"filter": {"bool": {"must": [
                    {"terms": {"customer_metadata_flattened.key.raw": [ "org" ] }},
                    {"terms": {"customer_metadata_flattened.value.raw": [ "ny" ] }}
                  ]}}, "path": "customer_metadata_flattened" }}]}
               },
               {"bool": {"should": {"nested": {"filter": {"bool": {"must": [
                 {"terms": {"customer_metadata_flattened.key.raw": [ "owner" ] }},
                 {"terms": {"customer_metadata_flattened.value.raw": [ "john" ] }}
               ]}}, "path": "customer_metadata_flattened" }}}}
             ]}
        }"""
      val actual = JsonReader.fromString(filter.get.toString)
      actual should be(expected)
    }
  }

  "DocumentFilters: moderationStatusFilter" should {
    val defaultFilter = j"""{ "term": { "is_default_view": true } }"""
    val approvedFilter = j"""{ "term": { "is_moderation_approved": true } }"""
    val defaultOrApprovedFilter = j"""{"bool": {"should": [$defaultFilter, $approvedFilter]}}"""
    val fromModeratedDomain = j"""{ "terms": { "socrata_id.domain_id": [ 1, 3 ] } }"""
    val fromUnmoderatedDomain = j"""{ "terms": { "socrata_id.domain_id": [ 0, 2 ] } }"""
    val fromNoDomain = j"""{ "terms" : { "socrata_id.domain_id" : [] } }"""
    val defaultOrApprovedFilterForUnmoderatedContext =
      j"""{"bool": {"should": [$defaultFilter, $approvedFilter, $fromNoDomain]}}"""
    val moderatedDomains = Set(1,3)
    val unmoderatedDomains = Set(0,2)

    "include default/approved views if the search context is not moderated and no domains are given" in {

      val filter = DocumentFilters.moderationStatusFilter(false, Set.empty, Set.empty)
      val actual = JsonReader.fromString(filter.toString)
      actual should be(defaultOrApprovedFilterForUnmoderatedContext)
    }

    "include default/approved views if the search context is not moderated and all domains given are moderated" in {
      val filter = DocumentFilters.moderationStatusFilter(false, moderatedDomains, Set.empty)
      val actual = JsonReader.fromString(filter.toString)
      actual should be(defaultOrApprovedFilterForUnmoderatedContext)
    }

    "include default/approved views from moderated sites and all else from unmoderated sites if the search context is not moderated" in {
      val filter = DocumentFilters.moderationStatusFilter(false, moderatedDomains, unmoderatedDomains)
      val expected = j"""{"bool": {"should": [$defaultFilter, $approvedFilter, $fromUnmoderatedDomain]}}"""
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }

    "include default/approved views if the search context is moderated and all domains are moderated" in {
      val filter = DocumentFilters.moderationStatusFilter(true, moderatedDomains, Set.empty)
      val actual = JsonReader.fromString(filter.toString)
      actual should be(defaultOrApprovedFilter)
    }

    "include default/approved views from moderated domains + default views from unmoderated domains if the search context is moderated" in {
      val filter = DocumentFilters.moderationStatusFilter(true, moderatedDomains, unmoderatedDomains)
      val beAcceptedFromModeratedDomain = j"""{"bool": {"must": [$fromModeratedDomain, $defaultOrApprovedFilter]}}"""
      val beDefaultFromUnoderatedDomain = j"""{"bool": {"must": [$fromUnmoderatedDomain, $defaultFilter]}} """
      val expected = j"""{"bool": {"should": [$beAcceptedFromModeratedDomain, $beDefaultFromUnoderatedDomain]}}"""
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }
  }

  "DocumentFilters: datalensStatusFilter" should {
    "include views that are not unapproved datalens" in {
      val filter = DocumentFilters.datalensStatusFilter(false)
      val datalensFilter = j"""{"terms" :{"datatype" :["datalens","datalens_chart","datalens_map"]}}"""
      val unApprovedFilter = j"""{"not" :{"filter" :{"term" :{"is_moderation_approved" : true}}}}"""
      val expected = j"""{"not" :{"filter" :{"bool" :{"must" :[$datalensFilter, $unApprovedFilter]}}}}"""
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }
  }

  "DocumentFilters: routingApprovalFilter" should {
    val raDisabledDomain = domains(0)
    val raEnabledDomain = domains(2)
    val approvedByParent = j"""{ "term": { "is_approved_by_parent_domain": true } }"""
    val fromRADisabledDomain = j"""{ "terms": { "socrata_id.domain_id": [ 0 ] } }"""
    val fromNoDomain = j"""{ "terms": { "socrata_id.domain_id": [ ] } }"""
    val approvedByParentNoSearchContext = j"""{ "bool" :{"should" :[$approvedByParent, $fromNoDomain]}}"""

    "include views that are approved by the parent domain if no search context is given and all domains have R&A" in {
       val filter = DocumentFilters.routingApprovalFilter(None, Set.empty)
       val expected = j"""{"bool": {"must": $approvedByParentNoSearchContext }}"""
       val actual = JsonReader.fromString(filter.toString)
       actual should be(expected)
    }

    "include views that are approved by the parent domain + any views from domains w/o RA if no search context is given" in {
      val filter = DocumentFilters.routingApprovalFilter(None, Set(raDisabledDomain.domainId))
      val expected = j"""{"bool": {"must": {"bool": {"should": [$approvedByParent, $fromRADisabledDomain]}} }}"""
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }

    "include views that are approved by the parent domain if the search context is given but has RA disabled and all domains have R&A" in {
      val filter = DocumentFilters.routingApprovalFilter(Some(domains(0)), Set.empty)
      val expected = j"""{"bool": {"must": $approvedByParentNoSearchContext }}"""
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }

    "include views that are approved by both the parent domain and the search_context if the search context has RA" in {
      val raEnabledEverywhereFilter = DocumentFilters.routingApprovalFilter(Some(raEnabledDomain), Set.empty)
      val raDisaledSomewhereFitler = DocumentFilters.routingApprovalFilter(Some(raEnabledDomain), Set(raDisabledDomain.domainId))
      val contextApproves = j"""{"terms" :{"approving_domain_ids" :[ ${raEnabledDomain.domainId}]}}"""
      val actualRaEnabledEverywhereFilter = JsonReader.fromString(raEnabledEverywhereFilter.toString)
      val actualRaDisaledSomewhereFitler = JsonReader.fromString(raDisaledSomewhereFitler.toString)
      val expectedRaEnabledEverywhereFilter =
        j"""{"bool": {"must": [ $approvedByParentNoSearchContext, $contextApproves ]}}"""
      val expectedRaDisaledSomewhereFitler =
        j"""{"bool": {"must": [
              {"bool": {"should": [$approvedByParent, $fromRADisabledDomain]}},
              $contextApproves
            ]}}"""

      actualRaEnabledEverywhereFilter should be(expectedRaEnabledEverywhereFilter)
      actualRaDisaledSomewhereFitler should be(expectedRaDisaledSomewhereFitler)
    }
  }

  "DocumentFilters: publicFilter" should {
    "return the expected filter" in {
      val filter = DocumentFilters.publicFilter(false)
      val actual = JsonReader.fromString(filter.toString)
      val expected =
        j"""{
        "not" : {
          "filter" : {
            "term" : {
              "is_public" : false
            }
          }
        }
      }"""
      actual should be(expected)
    }
  }

  "DocumentFilters: publishedFilter" should {
    "return the expected filter" in {
      val filter = DocumentFilters.publishedFilter(false)
      val actual = JsonReader.fromString(filter.toString)
      val expected =
        j"""{
        "not" : {
          "filter" : {
            "term" : {
              "is_published" : false
            }
          }
        }
      }"""
      actual should be(expected)
    }
  }

  "DocumentFilters: searchParamsFilter" should {
    "throw an unauthorizedError if there is no authenticated user looking for what is shared to another user" in {
      val searchParams = SearchParamSet(sharedTo = Some("ro-bear"))
      val user = None
      an [UnauthorizedError] should be thrownBy {
        DocumentFilters.searchParamsFilter(searchParams, user)
      }
    }

    "throw an unauthorizedError if there is an authenticated user is looking for what is shared to another user" in {
      val searchParams = SearchParamSet(sharedTo = Some("ro-bear"))
      val user = Some(User("anna-belle", flags = Some(List("admin"))))
      an [UnauthorizedError] should be thrownBy {
        DocumentFilters.searchParamsFilter(searchParams, user)
      }
    }

    "return the expected filter" in {
      val searchParams = SearchParamSet(
        searchQuery = SimpleQuery("search query terms"),
        domains = Some(Set("www.example.com", "test.example.com", "socrata.com")),
        searchContext = Some("www.search.me"),
        domainMetadata = Some(Set(("key", "value"))),
        categories = Some(Set("Social Services", "Environment", "Housing & Development")),
        tags = Some(Set("taxi", "art", "clowns")),
        datatypes = Some(Set("datasets")),
        user = Some("anna-belle"),
        sharedTo = Some("ro-bear"),
        attribution = Some("org"),
        parentDatasetId = Some("parent-id"),
        ids = Some(Set("id-one", "id-two"))
      )
      val user = User("ro-bear")
      val filter = DocumentFilters.searchParamsFilter(searchParams, Some(user)).get
      val actual = JsonReader.fromString(filter.toString)
      val expected = j"""{
        "bool" :
          {
            "must" :
              [
                { "terms" : { "datatype" : [ "dataset" ] } },
                { "term" : { "owner_id" : "anna-belle" } },
                { "term" : { "shared_to" : "ro-bear" } },
                { "term" : { "attribution.raw" : "org" } },
                { "term" : { "socrata_id.parent_dataset_id" : "parent-id" } },
                { "terms" : { "_id" : [ "id-one", "id-two" ] } },
                {"bool" :{"should" :{"nested" :{"filter" :{"bool" :{"must" :[
                  {"terms" :{"customer_metadata_flattened.key.raw" :[ "key" ]}},
                  {"terms" :{"customer_metadata_flattened.value.raw" :[ "value" ]}}
                ]}}, "path" : "customer_metadata_flattened"}}}},
                { "not" : { "filter" : { "term" : { "hide_from_catalog" : true }}}}
              ]
          }
      }
      """
      actual should be(expected)
    }
  }

  "DocumentFilters: anonymousFilter" should {
    "return the expected filter" in {
      val domainSet = DomainSet(Set(domains(0), domains(1)), Some(domains(1)))
      val filter = DocumentFilters.anonymousFilter(domainSet)
      val actual = JsonReader.fromString(filter.toString)
      val expected = j"""{
        "bool" :{
          "must" :[
            { "not" : { "filter" : { "term" : { "is_public" : false } } } },
            { "not" : { "filter" : { "term" : { "is_published" : false } } } },
            {"bool" :{"should" :[
              {"bool" :{"must" :[
                { "terms" : { "socrata_id.domain_id" : [ 1 ] } },
                {"bool" :{"should" :[
                  {"term" : {"is_default_view" : true}},
                  {"term" :{"is_moderation_approved" : true}}
                 ]}
                }]}},
              {"bool" :{"must" :[
                { "terms" : { "socrata_id.domain_id" : [ 0 ] } },
                { "term" : { "is_default_view" : true } }
              ]}}
            ]}},
            {"not" :{"filter" :{"bool" :{"must" :[
              {"terms" :{"datatype" :["datalens", "datalens_chart", "datalens_map"]}},
              {"not" :{"filter" :{"term" :{ "is_moderation_approved" : true }}}}
            ]}}}},
            {"bool" :{"must" :{"bool" :{"should" :[
              {"term" : {"is_approved_by_parent_domain" : true}},
              {"terms" : { "socrata_id.domain_id" : [ 0, 1 ] }}
            ]}}}}
          ]
        }
      }
      """
      actual should be(expected)
    }
  }

  "DocumentFilters: ownedOrSharedFilter" should {
    "return the expected filter" in {
      val user = User("mooks")
      val filter = DocumentFilters.ownedOrSharedFilter(user)
      val actual = JsonReader.fromString(filter.toString)
      val expected = j"""{
        "bool" :{"should" :[
          { "term" : { "owner_id" : "mooks" } },
          { "term" : { "shared_to" : "mooks" } }
        ]}
      }"""
      actual should be(expected)
    }
  }

  "DocumentFilters: chooseVisFilters" should {
    "throw an UnauthorizedError if no user is given and auth is required" in  {
      an [UnauthorizedError] should be thrownBy {
        DocumentFilters.visibilityFilter(None, DomainSet(), true)
      }
    }

    "return None for super admins if auth is required" in {
      val user = User("mooks", flags = Some(List("admin")))
      val domainSet = DomainSet(Set(domains(1)), Some(domains(1)))
      val filter = DocumentFilters.visibilityFilter(Some(user), domainSet, true)
      filter should be(None)
    }

    "return only anon and personal views for users who can view everything (but aren't super admins and don't have an authenticating domain) if auth is required" in {
      val user = User("mooks", roleName = Some("publisher"))
      val domainSet = DomainSet(Set(domains(1)), Some(domains(1)))
      val filter = DocumentFilters.visibilityFilter(Some(user), domainSet, true)
      val actual = JsonReader.fromString(filter.get.toString)
      val anonFilter = JsonReader.fromString(DocumentFilters.anonymousFilter(domainSet).toString)
      val personalFilter = JsonReader.fromString(DocumentFilters.ownedOrSharedFilter(user).toString)
      val expected = j"""{"bool": {"should": [$personalFilter, $anonFilter]}}"""
      actual should be(expected)
    }

    "return only anon, personal and within-domains views for users who can view everything and do have an authenticating domain if auth is required" in {
      val user = User("mooks", authenticatingDomain = Some(domains(2)), roleName = Some("publisher"))
      val domainSet = DomainSet(Set(domains(1)), Some(domains(1)))
      val filter = DocumentFilters.visibilityFilter(Some(user), domainSet, true)
      val actual = JsonReader.fromString(filter.get.toString)
      val anonFilter = JsonReader.fromString(DocumentFilters.anonymousFilter(domainSet).toString)
      val personalFilter = JsonReader.fromString(DocumentFilters.ownedOrSharedFilter(user).toString)
      val withinDomainFilter = JsonReader.fromString(DocumentFilters.domainIdFilter(Set(2)).toString)
      val expected = j"""{"bool": {"should": [$personalFilter, $anonFilter, $withinDomainFilter]}}"""
      actual should be(expected)
    }

    "return only anon and personal views for users who have logged in but cannot view everything if auth is required" in {
      val user = User("mooks", authenticatingDomain = Some(domains(1)), roleName = Some("editor"))
      val domainSet = DomainSet(Set(domains(1)), Some(domains(1)))
      val filter = DocumentFilters.visibilityFilter(Some(user), domainSet, true)
      val actual = JsonReader.fromString(filter.get.toString)
      val anonFilter = JsonReader.fromString(DocumentFilters.anonymousFilter(domainSet).toString)
      val personalFilter = JsonReader.fromString(DocumentFilters.ownedOrSharedFilter(user).toString)
      val expected = j"""{"bool": {"should": [$personalFilter, $anonFilter]}}"""
      actual should be(expected)
    }

    "return only anon views if auth is not required" in {
      val userWhoCannotViewAll = User("mooks", roleName = Some("editor"))
      val userWhoCanViewAllButNoDomain = User("mooks", roleName = Some("publisher"))
      val userWhoCanViewAllWithDomain = User("mooks", Some(domains(1)), roleName = Some("publisher"))
      val superAdmin = User("mooks", flags = Some(List("admin")))
      val domainSet = DomainSet(Set(domains(1)), Some(domains(1)))

      val filter1 = DocumentFilters.visibilityFilter(Some(userWhoCannotViewAll), domainSet, false)
      val filter2 = DocumentFilters.visibilityFilter(Some(userWhoCanViewAllButNoDomain), domainSet, false)
      val filter3 = DocumentFilters.visibilityFilter(Some(userWhoCanViewAllWithDomain), domainSet, false)
      val filter4 = DocumentFilters.visibilityFilter(Some(superAdmin), domainSet, false)

      val actual1 = JsonReader.fromString(filter1.get.toString)
      val actual2 = JsonReader.fromString(filter2.get.toString)
      val actual3 = JsonReader.fromString(filter3.get.toString)
      val actual4 = JsonReader.fromString(filter4.get.toString)
      val anonFilter = JsonReader.fromString(DocumentFilters.anonymousFilter(domainSet).toString)

      actual1 should be(anonFilter)
      actual2 should be(anonFilter)
      actual3 should be(anonFilter)
      actual4 should be(anonFilter)
    }
  }

  "UserFilters: idFilter" should {
    "return the expected filter" in {
      val filter = UserFilters.idFilter(Some(Set("foo-bar"))).get
      val actual = JsonReader.fromString(filter.toString)
      val expected = j"""{"terms" : {"id" : ["foo-bar"]}}"""
      actual should be(expected)
    }
  }

  "UserFilters: domainFilter" should {
    "return the expected filter" in {
      val filter = UserFilters.domainFilter(Some(1)).get
      val actual = JsonReader.fromString(filter.toString)
      val expected = j"""{"term" : {"roles.domain_id" : 1}}"""
      actual should be(expected)
    }
  }

  "UserFilters: roleFilter" should {
    "return the expected filter" in {
      val filter = UserFilters.roleFilter(Some(Set("muffin", "scone"))).get
      val actual = JsonReader.fromString(filter.toString)
      val expected = j"""{"terms" : {"roles.role_name" : ["muffin", "scone"]}}"""
      actual should be(expected)
    }
  }

  "UserFilters: emailFilter" should {
    "return the expected filter" in {
      val filter = UserFilters.emailFilter(Some(Set("foo@baz.bar"))).get
      val actual = JsonReader.fromString(filter.toString)
      val expected = j"""{"terms" : {"email.raw" : ["foo@baz.bar"]}}"""
      actual should be(expected)
    }
  }

  "UserFilters: screenNameFilter" should {
    "return the expected filter" in {
      val filter = UserFilters.screenNameFilter(Some(Set("nombre"))).get
      val actual = JsonReader.fromString(filter.toString)
      val expected = j"""{"terms" : {"screen_name.raw" : ["nombre"]}}"""
      actual should be(expected)
    }
  }

  "UserFilters: flagFilter" should {
    "return the expected filter" in {
      val filter = UserFilters.flagFilter(Some(Set("admin"))).get
      val actual = JsonReader.fromString(filter.toString)
      val expected = j"""{"terms" : {"flags" : ["admin"]}}"""
      actual should be(expected)
    }
  }

  "UserFilters: chooseVisFilters" should {
    "throw an UnauthorizedError if no user is given" in  {
      an [UnauthorizedError] should be thrownBy {
        UserFilters.visibilityFilter(None, None)
      }
    }

    "throw an UnauthorizedError if user's domain doesn't match given domain id" in  {
      val user = User("mooks", Some(domains(6)), Some("administrator"))
      an [UnauthorizedError] should be thrownBy {
        UserFilters.visibilityFilter(Some(user), Some(4))
      }
    }

    "throw an UnauthorizedError if user hasn't a role to view users" in  {
      val user = User("mooks", Some(domains(6)), Some(""))
      an [UnauthorizedError] should be thrownBy {
        UserFilters.visibilityFilter(Some(user), None)
      }
    }

    "return None for super admins" in {
      val user = User("mooks", flags = Some(List("admin")))
      val filter = UserFilters.visibilityFilter(Some(user), None)
      filter should be(None)
    }

    "return None for users who a) can view users, b) aren't snooping around other domains and c) aren't super admins" in {
      val user = User("mooks", Some(domains(1)), Some("administrator"))
      val filter = UserFilters.visibilityFilter(Some(user), None)
      filter should be(None)
    }
  }

  "UserFilters: compositeFilter" should {
    "return the expected filter" in {
      val params = UserSearchParamSet(
        emails = Some(Set("admin@gmail.com")),
        screenNames = Some(Set("Ad men")),
        flags = Some(Set("admin")),
        roles = Some(Set("admin"))
      )
      val user = User("", None, roleName = None, rights = None, flags = Some(List("admin")))
      val filter = UserFilters.compositeFilter(params, Some(1042), Some(user))
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
                        {"term": {"roles.domain_id": 1042}},
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
