package com.socrata.cetera.auth

import org.scalatest.{WordSpec, ShouldMatchers}

class UserSpec extends WordSpec with ShouldMatchers {
  val superAdmin = User("", None, None, Some(List("admin")))
  val customerAdmin = User("", Some("administrator"), None, None)
  val customerEditor = User("", Some("editor"), None, None)
  val customerPublisher = User("", Some("publisher"), None, None)
  val customerViewer = User("", Some("viewer"), None, None)
  val anonymous = User("", None, None, None)

  "The canViewLockedDownCatalog method" should {
    "return true if the user is a super admin" in {
      superAdmin.canViewLockedDownCatalog should be(true)
    }

    "return true if the user has a role" in {
      customerAdmin.canViewLockedDownCatalog should be(true)
      customerEditor.canViewLockedDownCatalog should be(true)
      customerPublisher.canViewLockedDownCatalog should be(true)
      customerViewer.canViewLockedDownCatalog should be(true)
    }

    "return false if the user is anonymous" in {
      anonymous.canViewLockedDownCatalog should be(false)
    }
  }

  "The canViewAdminDatasets method" should {
    "return true if the user is a super admin" in {
      superAdmin.canViewAdminDatasets should be(true)
    }

    "return true if the user has an elevated role" in {
      customerAdmin.canViewAdminDatasets should be(true)
      customerPublisher.canViewAdminDatasets should be(true)
    }

    "return false if the user has an other role" in {
      customerEditor.canViewAdminDatasets should be(false)
      customerViewer.canViewAdminDatasets should be(false)
    }

    "return false if the user is anonymous" in {
      anonymous.canViewAdminDatasets should be(false)
    }
  }

  "The canViewAssetSelector method" should {
    "return true if the user is a super admin" in {
      superAdmin.canViewAssetSelector should be(true)
    }

    "return true if the user has an elevated role" in {
      customerAdmin.canViewAssetSelector should be(true)
      customerPublisher.canViewAssetSelector should be(true)
    }

    "return true if the user has an other role" in {
      customerEditor.canViewAssetSelector should be(true)
      customerViewer.canViewAssetSelector should be(true)
    }

    "return false if the user is anonymous" in {
      anonymous.canViewAssetSelector should be(false)
    }
  }

  "The canViewUsers method" should {
    "return true if the use is a super admin" in {
      superAdmin.canViewUsers should be(true)
    }

    "return true if the use is a customer admin" in {
      customerAdmin.canViewUsers should be(true)
    }

    "return false if the user has an other role" in {
      customerEditor.canViewUsers should be(false)
      customerPublisher.canViewUsers should be(false)
      customerViewer.canViewUsers should be(false)
    }

    "return false if the user is anonymous" in {
      anonymous.canViewUsers should be(false)
    }
  }
}
