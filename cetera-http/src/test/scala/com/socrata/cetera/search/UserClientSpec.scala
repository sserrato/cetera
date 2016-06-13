package com.socrata.cetera.search

import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}

import com.socrata.cetera.types.{EsUser, Role}
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

  val expectedUser = EsUser(
    "soul-eater",
    Some("death-the-kid"),
    Some("death.kid@deathcity.com"),
    Some(Set(Role(0, "headmaster"))),
    Some("/api/users/soul-eater/profile_images/LARGE"),
    Some("/api/users/soul-eater/profile_images/THUMB"),
    Some("/api/users/soul-eater/profile_images/TINY")
  )

  test("fetch user by id") {
    val user = userClient.fetch("soul-eater")
    user should be(Some(expectedUser))
  }

  test("search returns all by default") {
    val (users, _) = userClient.search(None)
    users.headOption should be(Some(expectedUser))
  }

  test("search for user by exact name") {
    val (users, _) = userClient.search(Some("death-the-kid"))
    users.headOption should be(Some(expectedUser))
  }

  test("search for user by partial name") {
    val (users, _) = userClient.search(Some("death"))
    users.headOption should be(Some(expectedUser))
  }

  test("search for user by exact email") {
    val (users, _) = userClient.search(Some("death.kid@deathcity.com"))
    users.headOption should be(Some(expectedUser))
  }

  test("search for user by email alias") {
    val (users, _) = userClient.search(Some("death.kid"))
    users.headOption should be(Some(expectedUser))
  }

  test("search for user by email domain") {
    val (users, _) = userClient.search(Some("deathcity.com"))
    users.headOption should be(Some(expectedUser))
  }
}
