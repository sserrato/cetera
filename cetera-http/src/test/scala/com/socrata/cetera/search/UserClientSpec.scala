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
    val (userRes, _) = userClient.search(None, None, None)
    userRes should contain theSameElementsAs(users)
  }

  test("search for user by exact name") {
    val (userRes, _) = userClient.search(Some("death-the-kid"), None, None)
    userRes.head should be(users(1))
  }

  test("search for user by partial name") {
    val (userRes, _) = userClient.search(Some("kid"), None, None)
    userRes.head should be(users(1))
  }

  test("search for user by exact email") {
    val (userRes, _) = userClient.search(Some("death.kid@deathcity.com"), None, None)
    userRes.head should be(users(1))
  }

  test("search for user by email alias") {
    val (userRes, _) = userClient.search(Some("death.kid"), None, None)
    userRes.head should be(users(1))
  }

  test("search for user by email domain") {
    val (userRes, _) = userClient.search(Some("deathcity.com"), None, None)
    userRes should contain theSameElementsAs(List(users(1), users(2)))
  }

  test("search for users by non-existent role, get a None") {
    val (userRes, _) = userClient.search(None, Some("muffin"), None)
    userRes should be('empty)
  }

  test("search for users by role") {
    val (userRes, _) = userClient.search(None, Some("headmaster"), None)
    userRes.head should be(users(1))
  }

  test("search for users by non-existent domain, get a None") {
    val domain = Option(domains(1))
    val (userRes, _) = userClient.search(None, None, domain)
    userRes should be('empty)
  }

  test("search for users by domain") {
    val domain = Option(domains(0))
    val (userRes, _) = userClient.search(None, None, domain)
    userRes should contain theSameElementsAs(List(users(0), users(1), users(2)))
  }

  test("search for users by query and role") {
    val (userRes, _) = userClient.search(Some("death-the-kid"), Some("headmaster"), None)
    userRes.head should be(users(1))
  }

  test("search for users by query and domain") {
    val domain = Option(domains(0))
    val (userRes, _) = userClient.search(Some("death-the-kid"), None, domain)
    userRes.head should be(users(1))
  }

  test("search for users by role and domain") {
    val domain = Option(domains(0))
    val (userRes, _) = userClient.search(None, Some("headmaster"), domain)
    userRes.head should be(users(1))
  }

  test("search for users by query, role, and domain") {
    val domain = Option(domains(0))
    val (userRes, _) = userClient.search(Some("death-the-kid"), Some("headmaster"), domain)
    userRes.head should be(users(1))
  }
}
