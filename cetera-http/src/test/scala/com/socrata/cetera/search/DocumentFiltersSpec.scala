package com.socrata.cetera.search

import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.io.JsonReader
import org.scalatest.{ShouldMatchers, WordSpec}

import com.socrata.cetera.TestESDomains
import com.socrata.cetera.auth.User
import com.socrata.cetera.errors.UnauthorizedError
import com.socrata.cetera.handlers.{SearchParamSet, UserSearchParamSet}
import com.socrata.cetera.types.{ApprovalStatus, Domain, DomainSet, SimpleQuery}

class DocumentFiltersSpec extends WordSpec with ShouldMatchers with TestESDomains {

  "the datatypeFilter" should {
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

  "the userFilter" should {
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

  "the sharedToFilter" should {
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

  "the attributionFilter" should {
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

  "the provenanceFilter" should {
    "return the expected filter if some provenance is given" in {
      val attrFilter = DocumentFilters.provenanceFilter("official")
      val expected = j"""{ "term": { "provenance": "official" } }"""
      val actual = JsonReader.fromString(attrFilter.toString)
      actual should be(expected)
    }

    "return the expected filter if a prefix is used" in {
      val attrFilter = DocumentFilters.provenanceFilter("official", "document.")
      val expected = j"""{ "term": { "document.provenance": "official" } }"""
      val actual = JsonReader.fromString(attrFilter.toString)
      actual should be(expected)
    }
  }

  "the licenseFilter" should {
    "return the expected filter if some license is given" in {
      val attrFilter = DocumentFilters.licenseFilter("WTFPL")
      val expected = j"""{ "term": { "license": "WTFPL" } }"""
      val actual = JsonReader.fromString(attrFilter.toString)
      actual should be(expected)
    }

    "return the expected filter if a prefix is used" in {
      val attrFilter = DocumentFilters.licenseFilter("WTFPL", "document.")
      val expected = j"""{ "term": { "document.license": "WTFPL" } }"""
      val actual = JsonReader.fromString(attrFilter.toString)
      actual should be(expected)
    }
  }

  "the parentDatasetFilter" should {
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

  "the hideFromCatalogFilter" should {
    "return the expected filter" in {
      val filter = DocumentFilters.hideFromCatalogFilter()
      val expected = j"""{"not" :{"filter" :{"term" :{"hide_from_catalog" : true}}}}"""
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }
  }

  "the IdFilter" should {
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

  "the domainIdFilter" should {
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

  "the raStatusAccordingToParentDomainFilter" should {
    "return the expected filter when looking for approved and no prefix is given" in {
      val approvalFilter = DocumentFilters.raStatusAccordingToParentDomainFilter(ApprovalStatus.approved)
      val expected = j"""{ "term": { "is_approved_by_parent_domain": true } }"""
      val actual = JsonReader.fromString(approvalFilter.toString)
      actual should be(expected)
    }

    "return the expected filter when looking for approved and some prefix is given" in {
      val approvalFilter = DocumentFilters.raStatusAccordingToParentDomainFilter(ApprovalStatus.approved, "child_")
      val expected = j"""{ "term": { "child_is_approved_by_parent_domain": true } }"""
      val actual = JsonReader.fromString(approvalFilter.toString)
      actual should be(expected)
    }

    "return the expected filter when looking for rejected" in {
      val rejectedFilter = DocumentFilters.raStatusAccordingToParentDomainFilter(ApprovalStatus.rejected)
      val expected = j"""{ "term": { "is_rejected_by_parent_domain": true } }"""
      val actual = JsonReader.fromString(rejectedFilter.toString)
      actual should be(expected)
    }

    "return the expected filter when looking for pending" in {
      val pendingFilter = DocumentFilters.raStatusAccordingToParentDomainFilter(ApprovalStatus.pending)
      val expected = j"""{ "term": { "is_pending_on_parent_domain": true } }"""
      val actual = JsonReader.fromString(pendingFilter.toString)
      actual should be(expected)
    }
  }

  "the raStatusAccordingToContextFilter" should {
    "return the expected filter when looking for approved" in {
      val approvalFilter = DocumentFilters.raStatusAccordingToContextFilter(ApprovalStatus.approved, domains(3))
      val expected = j"""{ "terms": { "approving_domain_ids": [ 3 ] } }"""
      val actual = JsonReader.fromString(approvalFilter.toString)
      actual should be(expected)
    }

    "return the expected filter when looking for rejected" in {
      val rejectedFilter = DocumentFilters.raStatusAccordingToContextFilter(ApprovalStatus.rejected, domains(3))
      val expected = j"""{ "terms": { "rejecting_domain_ids": [ 3 ] } }"""
      val actual = JsonReader.fromString(rejectedFilter.toString)
      actual should be(expected)
    }

    "return the expected filter when looking for pending" in {
      val pendingFilter = DocumentFilters.raStatusAccordingToContextFilter(ApprovalStatus.pending, domains(3))
      val expected = j"""{ "terms": { "pending_domain_ids": [ 3 ] } }"""
      val actual = JsonReader.fromString(pendingFilter.toString)
      actual should be(expected)
    }
  }

  "the derivedFilter" should {
    "return the expected filter when no params are passed" in {
      val filter = DocumentFilters.derivedFilter()
      val actual = JsonReader.fromString(filter.toString)
      val expected = j"""{"term" : {"is_default_view" : false} }"""
      actual should be(expected)
    }

    "return the expected filter when a 'true' param passed" in {
      val filter = DocumentFilters.derivedFilter(true)
      val actual = JsonReader.fromString(filter.toString)
      val expected = j"""{"term" : {"is_default_view" : false} }"""
      actual should be(expected)
    }

    "return the expected filter when a 'false' param passed" in {
      val filter = DocumentFilters.derivedFilter(false)
      val actual = JsonReader.fromString(filter.toString)
      val expected = j"""{"term" : {"is_default_view" : true} }"""
      actual should be(expected)
    }
  }

  "the explicitlyHiddenFilter" should {
    "return the expected filter when no params are passed" in {
      val filter = DocumentFilters.explicitlyHiddenFilter()
      val actual = JsonReader.fromString(filter.toString)
      val expected = j"""{"term" : {"hide_from_catalog" : true} }"""
      actual should be(expected)
    }

    "return the expected filter when a 'true' param passed" in {
      val filter = DocumentFilters.explicitlyHiddenFilter(true)
      val actual = JsonReader.fromString(filter.toString)
      val expected = j"""{"term" : {"hide_from_catalog" : true} }"""
      actual should be(expected)
    }

    "return the expected filter when a 'false' param passed" in {
      val filter = DocumentFilters.explicitlyHiddenFilter(false)
      val actual = JsonReader.fromString(filter.toString)
      val expected = j"""{"term" : {"hide_from_catalog" : false} }"""
      actual should be(expected)
    }
  }

  "the privateMetadataUserRestrictionsFilter" should {
    "return the expected filter when the user doesn't have a blessed role" in {
      val user = User("user-fxf")
      val filter = DocumentFilters.privateMetadataUserRestrictionsFilter(user)
      val expected = j"""{ "bool":{ "should" :
              [ { "term" : { "owner_id" : "user-fxf" }},
                { "term" : { "shared_to" : "user-fxf" }}]}}"""
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }

    "return the expected filter when the user does have a blessed role" in {
      val user = User("user-fxf", Some(domains(0)), Some("publisher"))
      val filter = DocumentFilters.privateMetadataUserRestrictionsFilter(user)
      val expected = j"""{ "bool":{ "should" :
              [ { "term" : { "owner_id" : "user-fxf" }},
                { "term" : { "shared_to" : "user-fxf" }},
                { "terms": { "socrata_id.domain_id": [ 0 ]}}]}}"""
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }
  }

  "the domainMetadataFilter when public is true" should {
    "return the expected filter when an empty set of metadata is given" in {
      val filter = DocumentFilters.metadataFilter(Set.empty, public = true)
      filter should be(None)
    }

    "return the expected filter when a metadata query lists a single key, single value pair" in {
      val filter = DocumentFilters.metadataFilter(Set(("org", "ny")), public = true)
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
      val filter = DocumentFilters.metadataFilter(Set(("org", "ny"), ("org", "nj")), public = true)
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
      val filter = DocumentFilters.metadataFilter(Set(("org", "chicago"), ("owner", "john")), public = true)
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
      val filter = DocumentFilters.metadataFilter(Set(("org", "chicago"), ("owner", "john"), ("org", "ny")), public = true)
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

  "the privateDomainMetadataFilter" should {
    "return None if no user is given" in {
      val filter = DocumentFilters.privateMetadataFilter(Set.empty, None)
      filter should be(None)
    }

    "return None if the user doesn't have an authenticating domain" in {
      val user = User("mooks")
      val filter = DocumentFilters.privateMetadataFilter(Set.empty, Some(user))
      filter should be(None)
    }

    "return a basic metadata filter when the user is a super admin" in {
      val user = User("mooks", flags = Some(List("admin")))
      val metadata = Set(("org", "ny"))
      val actual = JsonReader.fromString(DocumentFilters.privateMetadataFilter(metadata, Some(user)).get.toString)
      val expected = JsonReader.fromString(DocumentFilters.metadataFilter(metadata, public = false).get.toString)
      actual should be(expected)
    }

    "return the expected filter when the user is authenticated" in {
      val user = User("mooks", Some(domains(0)), Some("publisher"))
      val metadata = Set(("org", "ny"), ("org", "nj"))
      val filter = DocumentFilters.privateMetadataFilter(metadata, Some(user))
      val metaFilter = JsonReader.fromString(DocumentFilters.metadataFilter(metadata, public = false).get.toString)
      val privacyFilter = JsonReader.fromString(DocumentFilters.privateMetadataUserRestrictionsFilter(user).toString)
      val expected = j"""{"bool": {"must": [ $privacyFilter, $metaFilter ]}}"""
      val actual = JsonReader.fromString(filter.get.toString)
      actual should be(expected)
    }
  }

  "the combinedMetadataFilter" should {
    "return None if neither public or private metadata filters are created" in {
      val user = User("mooks", flags = Some(List("admin")))
      val filter = DocumentFilters.combinedMetadataFilter(Set.empty, Some(user))
      filter should be(None)
    }

    "return only a public metadata filter if the user isn't authenticated" in {
      val user = User("mooks")
      val metadata = Set(("org", "ny"))
      val actual = JsonReader.fromString(DocumentFilters.combinedMetadataFilter(metadata, Some(user)).get.toString)
      val expected = JsonReader.fromString(DocumentFilters.metadataFilter(metadata, public = true).get.toString)
      actual should be(expected)
    }

    "return both public and private metadata filters if the user is authenticated" in {
      val user = User("mooks", Some(domains(0)), Some("publisher"))
      val metadata = Set(("org", "ny"), ("org", "nj"))
      val filter = DocumentFilters.combinedMetadataFilter(metadata, Some(user))
      val pubFilter = JsonReader.fromString(DocumentFilters.metadataFilter(metadata, public = true).get.toString)
      val privateFilter = JsonReader.fromString(DocumentFilters.privateMetadataFilter(metadata, Some(user)).get.toString)
      val expected = j"""{"bool": {"should": [ $pubFilter, $privateFilter ]}}"""
      val actual = JsonReader.fromString(filter.get.toString)
      actual should be(expected)
    }
  }

  "the vmSearchContextFilter" should {
    "return None if there is no search context" in {
      val domainSet = DomainSet(Set(domains(0)), None)
      val filter = DocumentFilters.vmSearchContextFilter(domainSet)
      filter should be(None)
    }

    "return None if the search context isn't moderated" in {
      val domainSet = DomainSet(Set(domains(0)), Some(domains(0)))
      val filter = DocumentFilters.vmSearchContextFilter(domainSet)
      filter should be(None)
    }

    "return the expected filter if the search context is moderated" in {
      val domainSet = DomainSet(Set(domains(1), domains(0)), Some(domains(1)))
      val defaultFilter = j"""{ "term": { "is_default_view": true } }"""
      val fromModeratedDomain = j"""{ "terms": { "socrata_id.domain_id": [ 1 ] } }"""
      val expected = j"""{"bool": {"should": [$defaultFilter, $fromModeratedDomain]}}"""
      val filter = DocumentFilters.vmSearchContextFilter(domainSet).get
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }
  }

  "the raSearchContextFilter" should {
    "return None if there is no search context" in {
      val domainSet = DomainSet(Set(domains(2)), None)
      val filter = DocumentFilters.raSearchContextFilter(domainSet)
      filter should be(None)
    }

    "return None if the search context doesn't have R&A enabled" in {
      val domainSet = DomainSet(Set(domains(1)), Some(domains(1)))
      val filter = DocumentFilters.raSearchContextFilter(domainSet)
      filter should be(None)
    }

    "return the expected filter if the search context does have R&A enabled" in {
      val domainSet = DomainSet(Set(domains(2), domains(3)), Some(domains(3)))
      val approved = j"""{ "terms": { "approving_domain_ids": [ 3 ] } }"""
      val rejected = j"""{ "terms": { "rejecting_domain_ids": [ 3 ] } }"""
      val pending = j"""{ "terms": { "pending_domain_ids": [ 3 ] } }"""
      val expected = j"""{"bool" : {"should" : [$approved, $rejected, $pending]}}"""
      val filter = DocumentFilters.raSearchContextFilter(domainSet).get
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }
  }

  "the searchContextFilter" should {
    "return None if there is no search context" in {
      val domainSet = DomainSet(Set(domains(2)), None)
      val filter = DocumentFilters.searchContextFilter(domainSet)
      filter should be(None)
    }

    "return None if the search context has neither R&A or VM enabled" in {
      val domainSet = DomainSet(Set(domains(1)), Some(domains(0)))
      val filter = DocumentFilters.searchContextFilter(domainSet)
      filter should be(None)
    }

    "return the expected filter if the search context has both R&A and VM enabled" in {
      val domainSet = DomainSet(Set(domains(2), domains(3)), Some(domains(3)))
      val vmFilter = JsonReader.fromString(DocumentFilters.vmSearchContextFilter(domainSet).get.toString())
      val raFilter = JsonReader.fromString(DocumentFilters.raSearchContextFilter(domainSet).get.toString())
      val expected = j"""{"bool" : {"must" : [$vmFilter, $raFilter]}}"""
      val filter = DocumentFilters.searchContextFilter(domainSet).get
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }
  }

  "the domainSetFilter" should {
    "return a basic domain filter if there is no search context" in {
      val domainSet = DomainSet(Set(domains(2)), None)
      val domainFilter = JsonReader.fromString(DocumentFilters.domainIdFilter(Set(2)).toString)
      val expected = j"""{"bool": {"must": $domainFilter}}"""
      val filter = DocumentFilters.domainSetFilter(domainSet)
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }

    "return a basic domain filter if the search context has neither R&A or VM enabled" in {
      val domainSet = DomainSet(Set(domains(1)), Some(domains(0)))
      val domainFilter = JsonReader.fromString(DocumentFilters.domainIdFilter(Set(1)).toString)
      val expected = j"""{"bool": {"must": $domainFilter}}"""
      val filter = DocumentFilters.domainSetFilter(domainSet)
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }

    "return the expected filter if the search context has both R&A and VM enabled" in {
      val domainSet = DomainSet(Set(domains(2), domains(3)), Some(domains(2)))
      val domainFilter = JsonReader.fromString(DocumentFilters.domainIdFilter(Set(2, 3)).toString)
      val contextFilter = JsonReader.fromString(DocumentFilters.searchContextFilter(domainSet).get.toString)
      val expected = j"""{"bool": {"must": [$domainFilter, $contextFilter]}}"""
      val filter = DocumentFilters.domainSetFilter(domainSet)
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }
  }

  "the moderationStatusFilter for approved" should {
    val defaultFilter = j"""{ "term": { "is_default_view": true } }"""
    val approvedFilter = j"""{ "term": { "moderation_status": "approved" } }"""

    "include only default/approved views if there are no unmoderated domains" in {
      val domainSet = DomainSet(Set(domains(1), domains(3)))
      val filter = DocumentFilters.moderationStatusFilter(ApprovalStatus.approved, domainSet)
      val fromNoDomain = j"""{ "terms" : { "socrata_id.domain_id" : [] } }"""
      val expected = j"""{"bool": {"should": [$defaultFilter, $approvedFilter, $fromNoDomain]}}"""
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }

    "include default/approved views + those from unmoderated domain if there are some unmoderated domains" in {
      val domainSet = DomainSet(Set(domains(0), domains(2), domains(3)))
      val filter = DocumentFilters.moderationStatusFilter(ApprovalStatus.approved, domainSet)
      val fromUnmoderatedDomain = j"""{ "terms": { "socrata_id.domain_id": [ 0, 2 ] } }"""
      val expected = j"""{"bool": {"should": [$defaultFilter, $approvedFilter, $fromUnmoderatedDomain]}}"""
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }
  }

  "the moderationStatusFilter for rejected" should {
    val rejectedFilter = j"""{ "term": { "moderation_status": "rejected" } }"""
    val beDerived = j"""{ "term" : { "is_default_view" : false } }"""

    "return the expected document-eliminating filter if there are no moderated domains" in {
      val domainSet = DomainSet(Set(domains(0), domains(2)))
      val fromNoDomain = j"""{ "terms" : { "socrata_id.domain_id" : [] } }"""
      val expected = j"""{"bool": {"must": [$fromNoDomain, $beDerived, $rejectedFilter]}}"""
      val filter = DocumentFilters.moderationStatusFilter(ApprovalStatus.rejected, domainSet)
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }

    "return rejected views from moderated domain if there are some moderated domains" in {
      val domainSet = DomainSet(Set(domains(1), domains(2), domains(3)))
      val filter = DocumentFilters.moderationStatusFilter(ApprovalStatus.rejected, domainSet)
      val fromModeratedDomain = j"""{ "terms": { "socrata_id.domain_id": [ 1, 3 ] } }"""
      val expected = j"""{"bool": {"must": [$fromModeratedDomain, $beDerived, $rejectedFilter]}}"""
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }
  }

  "the moderationStatusFilter for pending" should {
    val pendingFilter = j"""{ "term": { "moderation_status": "pending" } }"""
    val beDerived = j"""{ "term" : { "is_default_view" : false } }"""

    "return the expected document-eliminating filter if there are no moderated domains" in {
      val domainSet = DomainSet(Set(domains(0), domains(2)))
      val fromNoDomain = j"""{ "terms" : { "socrata_id.domain_id" : [] } }"""
      val expected = j"""{"bool": {"must": [$fromNoDomain, $beDerived, $pendingFilter]}}"""
      val filter = DocumentFilters.moderationStatusFilter(ApprovalStatus.pending, domainSet)
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }

    "return rejected views from moderated domain if there are some moderated domains" in {
      val domainSet = DomainSet(Set(domains(1), domains(2), domains(3)))
      val filter = DocumentFilters.moderationStatusFilter(ApprovalStatus.pending, domainSet)
      val fromModeratedDomain = j"""{ "terms": { "socrata_id.domain_id": [ 1, 3 ] } }"""
      val expected = j"""{"bool": {"must": [$fromModeratedDomain, $beDerived, $pendingFilter]}}"""
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }
  }

  "the datalensStatusFilter" should {
    val datalensFilter = j"""{"terms" :{"datatype" :["datalens","datalens_chart","datalens_map"]}}"""

    "include all views that are not unapproved datalens if looking for approved" in {
      val filter = DocumentFilters.datalensStatusFilter(ApprovalStatus.approved)
      val unApprovedFilter = j"""{"not" :{"filter" :{"term" :{"moderation_status" : "approved"}}}}"""
      val expected = j"""{"not" :{"filter" :{"bool" :{"must" :[$datalensFilter, $unApprovedFilter]}}}}"""
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }

    "include only rejected datalens if looking for rejected" in {
      val filter = DocumentFilters.datalensStatusFilter(ApprovalStatus.rejected)
      val rejectedFilter = j"""{ "term": { "moderation_status": "rejected" } }"""
      val expected = j"""{"bool": {"must": [$datalensFilter, $rejectedFilter]}}"""
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }

    "include only pending datalens if looking for pending" in {
      val filter = DocumentFilters.datalensStatusFilter(ApprovalStatus.pending)
      val pendingFilter = j"""{ "term": { "moderation_status": "pending" } }"""
      val expected = j"""{"bool": {"must": [$datalensFilter, $pendingFilter]}}"""
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }
  }

  "the raStatusFilter" should {
    val rejected = j"""{ "term": { "is_rejected_by_parent_domain": true } }"""
    val pending = j"""{ "term": { "is_pending_on_parent_domain": true } }"""
    val approved =  j"""{ "term": { "is_pending_on_parent_domain": true } }"""
    val fromCustomerDomainsWithRA = j"""{ "terms" : { "socrata_id.domain_id" : [4, 3, 2] } }"""

    "return the expected filter when looking for rejected regardless of context" in {
      val status = ApprovalStatus.rejected
      val domainSetNoContext = DomainSet((0 to 4).map(domains(_)).toSet, None)
      val domainSetRaDisabledContext = domainSetNoContext.copy(searchContext = Some(domains(0)))
      val domainSetRaEnabledContext = domainSetNoContext.copy(searchContext = Some(domains(2)))
      val expected = j"""{"bool": {"must": [$fromCustomerDomainsWithRA, $rejected]}}"""

      val filterNoContext = JsonReader.fromString(DocumentFilters.raStatusFilter(status, domainSetNoContext).toString)
      val filterRaDisabledContext = JsonReader.fromString(DocumentFilters.raStatusFilter(status, domainSetRaDisabledContext).toString)
      val filterRaEnabledContext = JsonReader.fromString(DocumentFilters.raStatusFilter(status, domainSetRaEnabledContext).toString)

      filterNoContext should be(expected)
      filterRaDisabledContext should be(expected)
      filterRaEnabledContext should be(expected)
    }

    "return the expected filter when looking for pending regardless of context" in {
      val status = ApprovalStatus.pending
      val domainSetNoContext = DomainSet((0 to 4).map(domains(_)).toSet, None)
      val domainSetRaDisabledContext = domainSetNoContext.copy(searchContext = Some(domains(0)))
      val domainSetRaEnabledContext = domainSetNoContext.copy(searchContext = Some(domains(2)))
      val expected = j"""{"bool": {"must": [$fromCustomerDomainsWithRA, $pending]}}"""

      val filterNoContext = JsonReader.fromString(DocumentFilters.raStatusFilter(status, domainSetNoContext).toString)
      val filterRaDisabledContext = JsonReader.fromString(DocumentFilters.raStatusFilter(status, domainSetRaDisabledContext).toString)
      val filterRaEnabledContext = JsonReader.fromString(DocumentFilters.raStatusFilter(status, domainSetRaEnabledContext).toString)

      filterNoContext should be(expected)
      filterRaDisabledContext should be(expected)
      filterRaEnabledContext should be(expected)
    }

    "return the expected filter when looking for approved regardless of context" in {
      val status = ApprovalStatus.approved
      val domainSetNoContext = DomainSet((0 to 4).map(domains(_)).toSet, None)
      val domainSetRaDisabledContext = domainSetNoContext.copy(searchContext = Some(domains(0)))
      val domainSetRaEnabledContext = domainSetNoContext.copy(searchContext = Some(domains(2)))
      val approvedByParent = j"""{ "term": { "is_approved_by_parent_domain": true } }"""
      val fromRADisabledDomain = j"""{ "terms": { "socrata_id.domain_id": [ 1, 0 ] } }"""
      val expected = j"""{"bool": {"should": [$fromRADisabledDomain, $approvedByParent]}}"""

      val filterNoContext = JsonReader.fromString(DocumentFilters.raStatusFilter(status, domainSetNoContext).toString)
      val filterRaDisabledContext = JsonReader.fromString(DocumentFilters.raStatusFilter(status, domainSetRaDisabledContext).toString)
      val filterRaEnabledContext = JsonReader.fromString(DocumentFilters.raStatusFilter(status, domainSetRaEnabledContext).toString)

      filterNoContext should be(expected)
      filterRaDisabledContext should be(expected)
      filterRaEnabledContext should be(expected)
    }
  }

  "the raStatusOnContextFilter" should {
    val someDomains = (0 to 4).map(domains(_)).toSet
    val raEnabledContext = Some(domains(2))
    val raDisabledContext = Some(domains(0))

    "return None if there is no context, regardless of status" in {
      val domainSet = DomainSet(someDomains, None)
      val approvedFilter = DocumentFilters.raStatusOnContextFilter(ApprovalStatus.approved, domainSet)
      val rejectedFilter = DocumentFilters.raStatusOnContextFilter(ApprovalStatus.rejected, domainSet)
      val pendingFilter = DocumentFilters.raStatusOnContextFilter(ApprovalStatus.pending, domainSet)

      approvedFilter should be(None)
      rejectedFilter should be(None)
      pendingFilter should be(None)
    }

    "return None if the context doesn't have R&A, regardless of status" in {
      val domainSet = DomainSet(someDomains, raDisabledContext)
      val approvedFilter = DocumentFilters.raStatusOnContextFilter(ApprovalStatus.approved, domainSet)
      val rejectedFilter = DocumentFilters.raStatusOnContextFilter(ApprovalStatus.rejected, domainSet)
      val pendingFilter = DocumentFilters.raStatusOnContextFilter(ApprovalStatus.pending, domainSet)

      approvedFilter should be(None)
      rejectedFilter should be(None)
      pendingFilter should be(None)
    }

    "return the expected filter when looking for approved on a search context with R&A" in {
      val domainSet = DomainSet(someDomains, raEnabledContext)
      val expected = j"""{ "terms": { "approving_domain_ids": [ 2 ] } }"""
      val filter = DocumentFilters.raStatusOnContextFilter(ApprovalStatus.approved, domainSet)
      val actual = JsonReader.fromString(filter.get.toString)
      actual should be(expected)
    }

    "return the expected filter when looking for rejected on a search context with R&A" in {
      val domainSet = DomainSet(someDomains, raEnabledContext)
      val expected = j"""{ "terms": { "rejecting_domain_ids": [ 2 ] } }"""
      val filter = DocumentFilters.raStatusOnContextFilter(ApprovalStatus.rejected, domainSet)
      val actual = JsonReader.fromString(filter.get.toString)
      actual should be(expected)
    }

    "return the expected filter when looking for pending on a search context with R&A" in {
      val domainSet = DomainSet(someDomains, raEnabledContext)
      val expected = j"""{ "terms": { "pending_domain_ids": [ 2 ] } }"""
      val filter = DocumentFilters.raStatusOnContextFilter(ApprovalStatus.pending, domainSet)
      val actual = JsonReader.fromString(filter.get.toString)
      actual should be(expected)
    }
  }

  "the approvalStatusFilter" should {
    val someDomains = (0 to 4).map(domains(_)).toSet
    val defaultFilter = j"""{ "term": { "is_default_view": true } }"""
    val approvedFilter = j"""{ "term": { "moderation_status" : "approved" } }"""
    val defaultOrApprovedFilter = j"""{"bool": {"should": [$defaultFilter, $approvedFilter]}}"""
    val fromModeratedDomain = j"""{ "terms": { "socrata_id.domain_id": [ 1, 3 ] } }"""
    val fromUnmoderatedDomain = j"""{ "terms": { "socrata_id.domain_id": [ 0, 2 ] } }"""
    val fromNoDomain = j"""{ "terms" : { "socrata_id.domain_id" : [] } }"""
    val defaultOrApprovedFilterForUnmoderatedContext =
      j"""{"bool": {"should": [$defaultFilter, $approvedFilter, $fromNoDomain]}}"""
    val moderatedDomains = Set(domains(1), domains(3))
    val unmoderatedDomains = Set(domains(0), domains(2))

    "return the expected filter for approved views if the status is approved and we include the context" in {
      val domainSet = DomainSet(someDomains, Some(domains(3)))
      val beModApproved = JsonReader.fromString(DocumentFilters.moderationStatusFilter(ApprovalStatus.approved, domainSet).toString)
      val beDatalensApproved = JsonReader.fromString(DocumentFilters.datalensStatusFilter(ApprovalStatus.approved).toString)
      val beRAApproved = JsonReader.fromString(DocumentFilters.raStatusFilter(ApprovalStatus.approved, domainSet).toString)
      val beRAApprovedOnContext = JsonReader.fromString(DocumentFilters.raStatusOnContextFilter(ApprovalStatus.approved, domainSet).get.toString)
      val expected = j"""{ "bool": { "must": [$beModApproved, $beDatalensApproved, $beRAApproved, $beRAApprovedOnContext]}}"""

      val filter = DocumentFilters.approvalStatusFilter(ApprovalStatus.approved, domainSet, includeContextApproval = true)
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }

    "return the expected filter for approved views if the status is approved and we exclude the context" in {
      val domainSet = DomainSet(someDomains, Some(domains(3)))
      val beModApproved = JsonReader.fromString(DocumentFilters.moderationStatusFilter(ApprovalStatus.approved, domainSet).toString)
      val beDatalensApproved = JsonReader.fromString(DocumentFilters.datalensStatusFilter(ApprovalStatus.approved).toString)
      val beRAApproved = JsonReader.fromString(DocumentFilters.raStatusFilter(ApprovalStatus.approved, domainSet).toString)
      val expected = j"""{ "bool": { "must": [$beModApproved, $beDatalensApproved, $beRAApproved]}}"""

      val filter = DocumentFilters.approvalStatusFilter(ApprovalStatus.approved, domainSet, includeContextApproval = false)
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }

    "return the expected filter for rejected views if the status is rejected and we include the context" in {
      val domainSet = DomainSet(someDomains, Some(domains(3)))
      val beModRejected = JsonReader.fromString(DocumentFilters.moderationStatusFilter(ApprovalStatus.rejected, domainSet).toString)
      val beDatalensRejected = JsonReader.fromString(DocumentFilters.datalensStatusFilter(ApprovalStatus.rejected).toString)
      val beRARejected = JsonReader.fromString(DocumentFilters.raStatusFilter(ApprovalStatus.rejected, domainSet).toString)
      val beRARejectedOnContext = JsonReader.fromString(DocumentFilters.raStatusOnContextFilter(ApprovalStatus.rejected, domainSet).get.toString)
      val expected = j"""{ "bool": { "should": [$beModRejected, $beDatalensRejected, $beRARejected, $beRARejectedOnContext]}}"""

      val filter = DocumentFilters.approvalStatusFilter(ApprovalStatus.rejected, domainSet, includeContextApproval = true)
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }

    "return the expected filter for rejected views if the status is rejected and we exclude the context" in {
      val domainSet = DomainSet(someDomains, Some(domains(3)))
      val beModRejected = JsonReader.fromString(DocumentFilters.moderationStatusFilter(ApprovalStatus.rejected, domainSet).toString)
      val beDatalensRejected = JsonReader.fromString(DocumentFilters.datalensStatusFilter(ApprovalStatus.rejected).toString)
      val beRARejected = JsonReader.fromString(DocumentFilters.raStatusFilter(ApprovalStatus.rejected, domainSet).toString)
      val expected = j"""{ "bool": { "should": [$beModRejected, $beDatalensRejected, $beRARejected]}}"""

      val filter = DocumentFilters.approvalStatusFilter(ApprovalStatus.rejected, domainSet, includeContextApproval = false)
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }

    "return the expected filter for pending views if the status is pending and we include the context" in {
      val domainSet = DomainSet(someDomains, Some(domains(3)))
      val beModPending = JsonReader.fromString(DocumentFilters.moderationStatusFilter(ApprovalStatus.pending, domainSet).toString)
      val beDatalensPending = JsonReader.fromString(DocumentFilters.datalensStatusFilter(ApprovalStatus.pending).toString)
      val beRAPending = JsonReader.fromString(DocumentFilters.raStatusFilter(ApprovalStatus.pending, domainSet).toString)
      val beRAPendingOnContext = JsonReader.fromString(DocumentFilters.raStatusOnContextFilter(ApprovalStatus.pending, domainSet).get.toString)
      val expected = j"""{ "bool": { "should": [$beModPending, $beDatalensPending, $beRAPending, $beRAPendingOnContext]}}"""

      val filter = DocumentFilters.approvalStatusFilter(ApprovalStatus.pending, domainSet, includeContextApproval = true)
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }

    "return the expected filter for pending views if the status is pending and we exclude the context" in {
      val domainSet = DomainSet(someDomains, Some(domains(3)))
      val beModPending = JsonReader.fromString(DocumentFilters.moderationStatusFilter(ApprovalStatus.pending, domainSet).toString)
      val beDatalensPending = JsonReader.fromString(DocumentFilters.datalensStatusFilter(ApprovalStatus.pending).toString)
      val beRAPending = JsonReader.fromString(DocumentFilters.raStatusFilter(ApprovalStatus.pending, domainSet).toString)
      val expected = j"""{ "bool": { "should": [$beModPending, $beDatalensPending, $beRAPending]}}"""

      val filter = DocumentFilters.approvalStatusFilter(ApprovalStatus.pending, domainSet, includeContextApproval = false)
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }
  }

  "the publicFilter" should {
    "return the expected filter when no params are passed" in {
      val filter = DocumentFilters.publicFilter()
      val actual = JsonReader.fromString(filter.toString)
      val expected = j"""{"term" : {"is_public" : true} }"""
      actual should be(expected)
    }

    "return the expected filter when a 'true' param passed" in {
      val filter = DocumentFilters.publicFilter(true)
      val actual = JsonReader.fromString(filter.toString)
      val expected = j"""{"term" : {"is_public" : true} }"""
      actual should be(expected)
    }

    "return the expected filter when a 'false' param passed" in {
      val filter = DocumentFilters.publicFilter(false)
      val actual = JsonReader.fromString(filter.toString)
      val expected = j"""{"term" : {"is_public" : false} }"""
      actual should be(expected)
    }
  }

  "the publishedFilter" should {
    "return the expected filter when no params are passed" in {
      val filter = DocumentFilters.publishedFilter()
      val actual = JsonReader.fromString(filter.toString)
      val expected = j"""{"term" : {"is_published" : true} }"""
      actual should be(expected)
    }

    "return the expected filter when a 'true' param passed" in {
      val filter = DocumentFilters.publishedFilter(true)
      val actual = JsonReader.fromString(filter.toString)
      val expected = j"""{"term" : {"is_published" : true} }"""
      actual should be(expected)
    }

    "return the expected filter when a 'false' param passed" in {
      val filter = DocumentFilters.publishedFilter(false)
      val actual = JsonReader.fromString(filter.toString)
      val expected = j"""{"term" : {"is_published" : false} }"""
      actual should be(expected)
    }
  }

  "the searchParamsFilter" should {
    "throw an unauthorizedError if there is no authenticated user looking for what is shared to another user" in {
      val searchParams = SearchParamSet(sharedTo = Some("ro-bear"))
      val user = None
      an[UnauthorizedError] should be thrownBy {
        DocumentFilters.searchParamsFilter(searchParams, user, DomainSet())
      }
    }

    "throw an unauthorizedError if there is an authenticated user is looking for what is shared to another user" in {
      val searchParams = SearchParamSet(sharedTo = Some("ro-bear"))
      val user = Some(User("anna-belle", flags = Some(List("admin"))))
      an[UnauthorizedError] should be thrownBy {
        DocumentFilters.searchParamsFilter(searchParams, user, DomainSet())
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
        ids = Some(Set("id-one", "id-two")),
        license = Some("GNU GPL")
      )
      val user = User("ro-bear")
      val filter = DocumentFilters.searchParamsFilter(searchParams, Some(user), DomainSet()).get
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
                { "term" : { "license" : "GNU GPL" } }
              ]
          }
      }
      """
      actual should be(expected)
    }
  }

  "the anonymousFilter" should {
    "return the expected filter" in {
      val domainSet = DomainSet((0 to 2).map(domains(_)).toSet, Some(domains(2)))
      val public = JsonReader.fromString(DocumentFilters.publicFilter(public = true).toString)
      val published = JsonReader.fromString(DocumentFilters.publishedFilter(published = true).toString)
      val approved = JsonReader.fromString(DocumentFilters.approvalStatusFilter(ApprovalStatus.approved, domainSet).toString)
      val unhidden = JsonReader.fromString(DocumentFilters.hideFromCatalogFilter().toString)
      val expected = j"""{ "bool": { "must": [$public, $published, $approved, $unhidden]}}"""
      val filter = DocumentFilters.anonymousFilter(domainSet)
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }
  }

  "the ownedOrSharedFilter" should {
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

  "the authFilter" should {
    val someDomains = (0 to 4).map(domains(_)).toSet
    val contextWithRA = Some(domains(3))

    "throw an UnauthorizedError if no user is given and auth is required" in  {
      an[UnauthorizedError] should be thrownBy {
        DocumentFilters.authFilter(None, DomainSet(), true)
      }
    }

    "return None for super admins if auth is required" in {
      val user = User("mooks", flags = Some(List("admin")))
      val domainSet = DomainSet(someDomains, contextWithRA)
      val filter = DocumentFilters.authFilter(Some(user), domainSet, true)
      filter should be(None)
    }

    "return anon (acknowleding context) and personal views for users who can view everything (but aren't super admins and don't have an authenticating domain) if auth is required" in {
      val user = User("mooks", roleName = Some("publisher"))
      val domainSet = DomainSet(someDomains, contextWithRA)
      val filter = DocumentFilters.authFilter(Some(user), domainSet, true)
      val actual = JsonReader.fromString(filter.get.toString)
      val anonFilter = JsonReader.fromString(DocumentFilters.anonymousFilter(domainSet, includeContextApproval = true).toString)
      val personalFilter = JsonReader.fromString(DocumentFilters.ownedOrSharedFilter(user).toString)
      val expected = j"""{"bool": {"should": [$personalFilter, $anonFilter]}}"""
      actual should be(expected)
    }

    "return anon views (disregarding context), personal views and within-domains views for users who can view everything and do have an authenticating domain if auth is required" in {
      val user = User("mooks", authenticatingDomain = contextWithRA, roleName = Some("publisher"))
      val domainSet = DomainSet(someDomains, contextWithRA)
      val filter = DocumentFilters.authFilter(Some(user), domainSet, true)
      val actual = JsonReader.fromString(filter.get.toString)
      val anonFilter = JsonReader.fromString(DocumentFilters.anonymousFilter(domainSet, includeContextApproval = false).toString)
      val personalFilter = JsonReader.fromString(DocumentFilters.ownedOrSharedFilter(user).toString)
      val withinDomainFilter = JsonReader.fromString(DocumentFilters.domainIdFilter(Set(3)).toString)
      val expected = j"""{"bool": {"should": [$personalFilter, $anonFilter, $withinDomainFilter]}}"""
      actual should be(expected)
    }

    "return anon (acknowleding context) and personal views for users who have logged in but cannot view everything if auth is required" in {
      val user = User("mooks", authenticatingDomain = contextWithRA, roleName = Some("editor"))
      val domainSet = DomainSet(someDomains, contextWithRA)
      val filter = DocumentFilters.authFilter(Some(user), domainSet, true)
      val actual = JsonReader.fromString(filter.get.toString)
      val anonFilter = JsonReader.fromString(DocumentFilters.anonymousFilter(domainSet, includeContextApproval = true).toString)
      val personalFilter = JsonReader.fromString(DocumentFilters.ownedOrSharedFilter(user).toString)
      val expected = j"""{"bool": {"should": [$personalFilter, $anonFilter]}}"""
      actual should be(expected)
    }

    "return only anon (acknowleding context) views if auth is not required" in {
      val userWhoCannotViewAll = User("mooks", roleName = Some("editor"))
      val userWhoCanViewAllButNoDomain = User("mooks", roleName = Some("publisher"))
      val userWhoCanViewAllWithDomain = User("mooks", contextWithRA, roleName = Some("publisher"))
      val superAdmin = User("mooks", flags = Some(List("admin")))
      val domainSet = DomainSet(someDomains, contextWithRA)

      val filter1 = DocumentFilters.authFilter(Some(userWhoCannotViewAll), domainSet, false)
      val filter2 = DocumentFilters.authFilter(Some(userWhoCanViewAllButNoDomain), domainSet, false)
      val filter3 = DocumentFilters.authFilter(Some(userWhoCanViewAllWithDomain), domainSet, false)
      val filter4 = DocumentFilters.authFilter(Some(superAdmin), domainSet, false)

      val actual1 = JsonReader.fromString(filter1.get.toString)
      val actual2 = JsonReader.fromString(filter2.get.toString)
      val actual3 = JsonReader.fromString(filter3.get.toString)
      val actual4 = JsonReader.fromString(filter4.get.toString)
      val anonFilter = JsonReader.fromString(DocumentFilters.anonymousFilter(domainSet, includeContextApproval = true).toString)

      actual1 should be(anonFilter)
      actual2 should be(anonFilter)
      actual3 should be(anonFilter)
      actual4 should be(anonFilter)
    }
  }
}
