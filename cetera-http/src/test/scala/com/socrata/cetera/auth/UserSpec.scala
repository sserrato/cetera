package com.socrata.cetera.auth

import org.scalatest.{ShouldMatchers, WordSpec}

import com.socrata.cetera.TestESDomains

class UserSpec extends WordSpec with ShouldMatchers with TestESDomains {
  val superAdmin = User("", None, roleName = None, rights = None, flags = Some(List("admin")))
  val customerAdmin = User("", Some(domains(8)), roleName = Some("administrator"), rights = None, flags = None)
  val customerEditor = User("", Some(domains(8)), roleName = Some("editor"), rights = None, flags = None)
  val customerEditorStories = User("", Some(domains(8)), roleName = Some("editor_stories"), rights = None, flags = None)
  val customerPublisher = User("", Some(domains(8)), roleName = Some("publisher"), rights = None, flags = None)
  val customerPublisherStories = User("", Some(domains(8)), roleName = Some("publisher_stories"), rights = None, flags = None)
  val customerViewer = User("", Some(domains(8)), roleName = Some("viewer"), rights = None, flags = None)
  val customerDesigner = User("", Some(domains(8)), roleName = Some("designer"), rights = None, flags = None)
  val anonymous = User("", None, roleName = None, rights = None, flags = None)
  val adminWithoutAuthenticatingDomain = User("", None, roleName = Some("administrator"), rights = None, flags = None)

  "The authorizedOnDomain method" should {
    "return false if the user has no authenticating domain" in  {
      domains.foreach { d =>
        adminWithoutAuthenticatingDomain.authorizedOnDomain(d) should be(false)
      }
    }

    "return false if the user has an authenticating domain but is trying to authenticate on a different domain" in  {
      customerAdmin.authorizedOnDomain(domains(1)) should be(false)
    }

    "return true if the user has an authenticating domain and is trying to authenticate on that domain" in  {
      customerAdmin.authorizedOnDomain(domains(8)) should be(true)
    }
  }

  "The canViewResource method" should {
    "return false if the user has no authenticating domain and is not a super admin" in  {
      adminWithoutAuthenticatingDomain.canViewResource(domains(1), isAuthorized = true) should be(false)
    }

    "return false if the user has an authenticating domain but is trying to authenticate on a different domain" in  {
      customerAdmin.canViewResource(domains(1), isAuthorized = true) should be(false)
    }

    "return false if the user has an authenticating domain and is trying to authenticate on that domain but is not authorized" in  {
      customerAdmin.canViewResource(domains(8), isAuthorized = false) should be(false)
    }

    "return true if the user is a super admin" in  {
      domains.foreach { d =>
        superAdmin.canViewResource(d, isAuthorized = false) should be(true)
      }
    }

    "return true if the user has an authenticating domain and is trying to authenticate on that domain and is authorized" in  {
      customerAdmin.canViewResource(domains(8), isAuthorized = true) should be(true)
    }
  }

  "The canViewLockedDownCatalog method" should {
    "return true if the user is a super admin" in {
      superAdmin.canViewLockedDownCatalog(domains(6)) should be(true)
    }

    "return true if the user has a supported role from the authenticating domain" in {
      customerAdmin.canViewLockedDownCatalog(domains(8)) should be(true)
      customerEditor.canViewLockedDownCatalog(domains(8)) should be(true)
      customerEditorStories.canViewLockedDownCatalog(domains(8)) should be(true)
      customerPublisher.canViewLockedDownCatalog(domains(8)) should be(true)
      customerPublisherStories.canViewLockedDownCatalog(domains(8)) should be(true)
      customerViewer.canViewLockedDownCatalog(domains(8)) should be(true)
    }

    "return false if the user has a role from the authenticating domain but it isn't supported" in {
      customerDesigner.canViewLockedDownCatalog(domains(8)) should be(false)
    }

    "return false if the user has a role but it isn't from the authenticating domain" in {
      customerAdmin.canViewLockedDownCatalog(domains(7)) should be(false)
      customerEditor.canViewLockedDownCatalog(domains(7)) should be(false)
      customerEditorStories.canViewLockedDownCatalog(domains(7)) should be(false)
      customerPublisher.canViewLockedDownCatalog(domains(7)) should be(false)
      customerPublisherStories.canViewLockedDownCatalog(domains(7)) should be(false)
      customerViewer.canViewLockedDownCatalog(domains(7)) should be(false)
      customerDesigner.canViewLockedDownCatalog(domains(7)) should be(false)
    }

    "return false if the user is anonymous" in {
      anonymous.canViewLockedDownCatalog(domains(8)) should be(false)
    }
  }

  "The canViewAllViews method" should {
    "return true if the user is a super admin" in {
      superAdmin.canViewAllViews(domains(6)) should be(true)
    }

    "return true if the user has a supported role from the authenticating domain" in {
      customerAdmin.canViewAllViews(domains(8)) should be(true)
      customerDesigner.canViewAllViews(domains(8)) should be(true)
      customerPublisher.canViewAllViews(domains(8)) should be(true)
      customerPublisherStories.canViewAllViews(domains(8)) should be(true)
      customerViewer.canViewAllViews(domains(8)) should be(true)
    }

    "return false if the user has a role from the authenticating domain but it isn't supported" in {
      customerEditor.canViewAllViews(domains(8)) should be(false)
      customerEditorStories.canViewAllViews(domains(8)) should be(false)
    }

    "return false if the user has a role but it isn't from the authenticating domain" in {
      customerAdmin.canViewAllViews(domains(7)) should be(false)
      customerEditor.canViewAllViews(domains(7)) should be(false)
      customerEditorStories.canViewAllViews(domains(7)) should be(false)
      customerPublisher.canViewAllViews(domains(7)) should be(false)
      customerPublisherStories.canViewAllViews(domains(7)) should be(false)
      customerViewer.canViewAllViews(domains(7)) should be(false)
      customerDesigner.canViewAllViews(domains(7)) should be(false)
    }

    "return false if the user is anonymous" in {
      anonymous.canViewAllViews(domains(8)) should be(false)
    }
  }

  "The canViewUsers method" should {
    "return true if the use is a super admin" in {
      superAdmin.canViewAllUsers should be(true)
    }

    "return true if the user is a customer admin" in {
      customerAdmin.canViewAllUsers should be(true)
    }

    "return false if the user has any other role" in {
      customerEditor.canViewAllUsers should be(false)
      customerEditorStories.canViewAllUsers should be(false)
      customerPublisher.canViewAllUsers should be(false)
      customerPublisherStories.canViewAllUsers should be(false)
      customerViewer.canViewAllUsers should be(false)
    }

    "return false if the user is anonymous" in {
      anonymous.canViewAllUsers should be(false)
    }
  }
}
