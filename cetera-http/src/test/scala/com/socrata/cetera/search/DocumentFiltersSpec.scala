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

  "the isApprovedByParentDomainFilter" should {
    "return the expected filter when no prefix is given" in {
      val approvalFilter = DocumentFilters.approvedByParentDomainFilter()
      val expected = j"""{ "term": { "is_approved_by_parent_domain": true } }"""
      val actual = JsonReader.fromString(approvalFilter.toString)
      actual should be(expected)
    }

    "return the expected filter when some prefix is given" in {
      val approvalFilter = DocumentFilters.approvedByParentDomainFilter("child_")
      val expected = j"""{ "term": { "child_is_approved_by_parent_domain": true } }"""
      val actual = JsonReader.fromString(approvalFilter.toString)
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

  "the domainMetadataFilter" should {
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
      val expected = j"""{"bool" : {"should" : {"terms" : {"approving_domain_ids" : [ 3 ]}}}}"""
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
      val approvedByContext = j"""{ "terms": { "approving_domain_ids": [ 2 ] } }"""
      // TODO: add in other means of being in the contexts queue when have that info
      // val rejectedByContext = ???
      // val pending = ???
      val expected = j"""{"bool": {"should": $approvedByContext }}"""
      val domainSet = DomainSet(Set(domains(2), domains(3)), Some(domains(2)))
      val filter = DocumentFilters.raSearchContextFilter(domainSet).get
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

    "return the expected document-eliminating filter if there are no moderated domains" in {
      val domainSet = DomainSet(Set(domains(0), domains(2)))
      val fromNoDomain = j"""{ "terms" : { "socrata_id.domain_id" : [] } }"""
      val expected = j"""{"bool": {"must": [$fromNoDomain, $rejectedFilter]}}"""
      val filter = DocumentFilters.moderationStatusFilter(ApprovalStatus.rejected, domainSet)
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }

    "return rejected views from moderated domain if there are some moderated domains" in {
      val domainSet = DomainSet(Set(domains(1), domains(2), domains(3)))
      val filter = DocumentFilters.moderationStatusFilter(ApprovalStatus.rejected, domainSet)
      val fromModeratedDomain = j"""{ "terms": { "socrata_id.domain_id": [ 1, 3 ] } }"""
      val expected = j"""{"bool": {"must": [$fromModeratedDomain, $rejectedFilter]}}"""
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }
  }

  "the moderationStatusFilter for pending" should {
    val pendingFilter = j"""{ "term": { "moderation_status": "pending" } }"""

    "return the expected document-eliminating filter if there are no moderated domains" in {
      val domainSet = DomainSet(Set(domains(0), domains(2)))
      val fromNoDomain = j"""{ "terms" : { "socrata_id.domain_id" : [] } }"""
      val expected = j"""{"bool": {"must": [$fromNoDomain, $pendingFilter]}}"""
      val filter = DocumentFilters.moderationStatusFilter(ApprovalStatus.pending, domainSet)
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }

    "return rejected views from moderated domain if there are some moderated domains" in {
      val domainSet = DomainSet(Set(domains(1), domains(2), domains(3)))
      val filter = DocumentFilters.moderationStatusFilter(ApprovalStatus.pending, domainSet)
      val fromModeratedDomain = j"""{ "terms": { "socrata_id.domain_id": [ 1, 3 ] } }"""
      val expected = j"""{"bool": {"must": [$fromModeratedDomain, $pendingFilter]}}"""
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

  "the routingApprovalFilter" should {
    val raDisabledDomain = domains(0)
    val raEnabledDomain = domains(2)
    val bothTypesOfDomain = Set(raDisabledDomain, raEnabledDomain)
    val approvedByParent = j"""{ "term": { "is_approved_by_parent_domain": true } }"""
    val fromRADisabledDomain = j"""{ "terms": { "socrata_id.domain_id": [ 0 ] } }"""
    val fromNoDomain = j"""{ "terms": { "socrata_id.domain_id": [ ] } }"""
    val approvedByParentNoSearchContext = j"""{ "bool" :{"should" :[$approvedByParent, $fromNoDomain]}}"""

    "include views that are approved by the parent domain if no search context is given & all domains have RA-enabled" in {
      val domainSet = DomainSet(Set(raEnabledDomain), None)
      val filter = DocumentFilters.routingApprovalFilter(domainSet)
      val expected = j"""{"bool": {"must": {"bool": {"should": [$approvedByParent, $fromNoDomain]}} }}"""
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }

    "include views that are approved by the parent domain + any views from domains w/o RA if no search context is given" in {
      val domainSet = DomainSet(bothTypesOfDomain, None)
      val filter = DocumentFilters.routingApprovalFilter(domainSet)
      val expected = j"""{"bool": {"must": {"bool": {"should": [$approvedByParent, $fromRADisabledDomain]}} }}"""
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }

    "include views that are approved by the parent domain + any views from domains w/o RA if the search context is given but has RA disabled" in {
      val domainSet = DomainSet(bothTypesOfDomain, Some(raDisabledDomain))
      val filter = DocumentFilters.routingApprovalFilter(domainSet)
      val expected = j"""{"bool": {"must": {"bool": {"should": [$approvedByParent, $fromRADisabledDomain]}} }}"""
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }

    "include views that are approved by both the parent domain and the search_context if the search context has RA" in {
      val domainSet = DomainSet(bothTypesOfDomain, Some(raEnabledDomain))
      val filter = DocumentFilters.routingApprovalFilter(domainSet)
      val contextApproves = j"""{"terms" :{"approving_domain_ids" :[ ${raEnabledDomain.domainId}]}}"""
      val expected = j"""
        {"bool": {"must": [
          {"bool": {"should": [$approvedByParent, $fromRADisabledDomain]}},
          $contextApproves
        ]}}"""
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }
  }

  "the raStatusFilter" should {
    "be the same as the raApprovalFilter if looking for approved" in {
      val domainSet = DomainSet((0 to 4).map(domains(_)).toSet, Some(domains(2)))
      val expected = JsonReader.fromString(DocumentFilters.routingApprovalFilter(domainSet).toString)
      val filter = DocumentFilters.raStatusFilter(ApprovalStatus.approved, domainSet)
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }

    "have testing for the rejected case" in {
      // TODO
    }

    "have testing for the pending case" in {
      // TODO
    }
  }

  "the approvalStatusFilter" should {
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

    "return the composite of viewModeration, R&A and datalens approval if the status is approved" in {
      val domainSet = DomainSet(unmoderatedDomains, Some(domains(0)))
      val beModApproved = JsonReader.fromString(DocumentFilters.moderationStatusFilter(ApprovalStatus.approved, domainSet).toString)
      val beDatalensApproved = JsonReader.fromString(DocumentFilters.datalensStatusFilter(ApprovalStatus.approved).toString)
      val beRAApproved = JsonReader.fromString(DocumentFilters.raStatusFilter(ApprovalStatus.approved, domainSet).toString)
      val expected = j"""{ "bool": { "must": [$beModApproved, $beDatalensApproved, $beRAApproved]}}"""

      val filter = DocumentFilters.approvalStatusFilter(ApprovalStatus.approved, domainSet)
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }

    "return a filter for rejected views/datalens if the status is rejected" in {
      val domainSet = DomainSet(moderatedDomains, Some(domains(1)))
      val beModRejected = JsonReader.fromString(DocumentFilters.moderationStatusFilter(ApprovalStatus.rejected, domainSet).toString)
      val beDatalensRejected = JsonReader.fromString(DocumentFilters.datalensStatusFilter(ApprovalStatus.rejected).toString)
      val beRARejected = JsonReader.fromString(DocumentFilters.raStatusFilter(ApprovalStatus.rejected, domainSet).toString)
      val expected = j"""{ "bool": { "should": [$beModRejected, $beDatalensRejected, $beRARejected]}}"""

      val filter = DocumentFilters.approvalStatusFilter(ApprovalStatus.rejected, domainSet)
      val actual = JsonReader.fromString(filter.toString)
      actual should be(expected)
    }

    "return a filter for pending views/datalens if the status is pending" in {
      val domainSet = DomainSet(moderatedDomains, Some(domains(1)))
      val beModPending = JsonReader.fromString(DocumentFilters.moderationStatusFilter(ApprovalStatus.pending, domainSet).toString)
      val beDatalensPending = JsonReader.fromString(DocumentFilters.datalensStatusFilter(ApprovalStatus.pending).toString)
      val beRAPending = JsonReader.fromString(DocumentFilters.raStatusFilter(ApprovalStatus.pending, domainSet).toString)
      val expected = j"""{ "bool": { "should": [$beModPending, $beDatalensPending, $beRAPending]}}"""

      val filter = DocumentFilters.approvalStatusFilter(ApprovalStatus.pending, domainSet)
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
        ids = Some(Set("id-one", "id-two"))
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
                ]}}, "path" : "customer_metadata_flattened"}}}}
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

  "the chooseVisFilters" should {
    "throw an UnauthorizedError if no user is given and auth is required" in  {
      an[UnauthorizedError] should be thrownBy {
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
}
