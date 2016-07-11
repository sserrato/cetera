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
    val userRes = userClient.fetch("dead-beef")
    userRes should be('empty)
  }

  test("fetch user by id") {
    val userRes = userClient.fetch("soul-eater")
    userRes should be(Some(users(1)))
  }

  test("search returns all by default") {
    val (userRes, _) = userClient.search(None)
    userRes should contain theSameElementsAs(users)
  }

  test("search for user by exact name") {
    val (userRes, _) = userClient.search(Some("death-the-kid"))
    userRes.head should be(users(1))
  }

  test("search for user by partial name") {
    val (userRes, _) = userClient.search(Some("kid"))
    userRes.head should be(users(1))
  }

  test("search for user by exact email") {
    val (userRes, _) = userClient.search(Some("death.kid@deathcity.com"))
    userRes.head should be(users(1))
  }

  test("search for user by email alias") {
    val (userRes, _) = userClient.search(Some("death.kid"))
    userRes.head should be(users(1))
  }

  test("search for user by email domain") {
    val (userRes, _) = userClient.search(Some("deathcity.com"))
    userRes should contain theSameElementsAs(List(users(1), users(2)))
  }
}
