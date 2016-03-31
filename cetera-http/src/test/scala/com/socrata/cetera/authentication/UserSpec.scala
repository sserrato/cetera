package com.socrata.cetera.authentication

import org.scalatest.{WordSpec, ShouldMatchers}

class UserSpec extends WordSpec with ShouldMatchers {

  "The canViewCatalog method" should {
    "return true if the user is an admin" in {
      val user = User("", "", None, None, Some(List("admin")))
      user.canViewCatalog should be(true)
    }

    "return true if the user has a role" in {
      val user = User("", "", Some("administrator"), None, None)
      user.canViewCatalog should be(true)
    }

    "return false if the user isn't an admin and hasn't a role" in {
      val user = User("", "", None, None, None)
      user.canViewCatalog should be(false)
    }
  }
}
