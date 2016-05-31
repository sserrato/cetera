package com.socrata.cetera.search

import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}

import com.socrata.cetera.{TestESClient, TestESData}

class UserClientSpec extends FunSuiteLike with Matchers with TestESData with BeforeAndAfterAll {
  val client = new TestESClient(testSuiteName)
  val userClient = new UserClient(client, testSuiteName)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    bootstrapData()
  }

  override protected def afterAll(): Unit = {
    removeBootstrapData()
    client.close()
    super.beforeAll()
  }

  test("fetch user by non-existent id, get a None") {
    val user = userClient.fetch("dead-beef")
    user should be('empty)
  }

  test("fetch user by id") {
    val expectedId = "soul-eater"
    val user = userClient.fetch(expectedId)

    user should be('defined)
    user.get.id should be(expectedId)

    user.get.screenName should be('defined)
    user.get.screenName.get should be("death-the-kid")

    user.get.email should be('defined)
    user.get.email.get should be("death.kid@deathcity.com")

    user.get.roleName should be('defined)
    user.get.roleName.get should be("headmaster")

    user.get.profileImageUrlLarge should be('defined)
    user.get.profileImageUrlLarge.get should be("/api/users/soul-eater/profile_images/LARGE")

    user.get.profileImageUrlMedium should be('defined)
    user.get.profileImageUrlMedium.get should be("/api/users/soul-eater/profile_images/THUMB")

    user.get.profileImageUrlSmall should be('defined)
    user.get.profileImageUrlSmall.get should be("/api/users/soul-eater/profile_images/TINY")
  }

  test("search returns all by default") {
    val (users, _) = userClient.search(None)
    users.headOption should be('defined)
  }

  test("search for user by exact name") {
    val (users, _) = userClient.search(Some("death-the-kid"))
    users.headOption should be('defined)
  }

  test("search for user by partial name") {
    val (users, _) = userClient.search(Some("death"))
    users.headOption should be('defined)
  }

  test("search for user by exact email") {
    val (users, _) = userClient.search(Some("death.kid@deathcity.com"))
    users.headOption should be('defined)
  }

  test("search for user by email alias") {
    val (users, _) = userClient.search(Some("death.kid"))
    users.headOption should be('defined)
  }

  test("search for user by email domain") {
    val (users, _) = userClient.search(Some("deathcity.com"))
    users.headOption should be('defined)
  }
}
