package com.socrata.cetera.search

import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}

import com.socrata.cetera.handlers.{PagingParamSet, UserSearchParamSet}
import com.socrata.cetera.{TestESClient, TestESData}

class UserClientSpec extends FunSuiteLike with Matchers with TestESData with BeforeAndAfterAll {
  val client = new TestESClient(testSuiteName)
  val userClient = new UserClient(client, testSuiteName)

  val pagingParams = PagingParamSet(0, 200, None)
  val baseSearchParams = UserSearchParamSet.empty

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
    val (userRes, _) = userClient.search(baseSearchParams, pagingParams, None)
    userRes should contain theSameElementsAs(users)
  }

  test("search by singular role") {
    val params = baseSearchParams.copy(roles = Some(Set("bear")))
    val (userRes, _) = userClient.search(params, pagingParams, None)
    userRes should contain theSameElementsAs (Seq(users(3), users(4)))
  }

  test("search by multiple roles") {
    val params = baseSearchParams.copy(roles = Some(Set("bear", "headmaster")))
    val (userRes, _) = userClient.search(params, pagingParams, None)
    userRes should contain theSameElementsAs (Seq(users(1), users(3), users(4)))
  }

  test("search by domain") {
    val params = baseSearchParams.copy(domain = Some("domain.that.will.be.ignored.com"))
    val (userRes, _) = userClient.search(params, pagingParams, Some(1))
    userRes should contain theSameElementsAs (Seq(users(3), users(4)))
  }

  test("search by singular email") {
    val params = baseSearchParams.copy(emails = Some(Set("funshine.bear@care.alot")))
    val (userRes, _) = userClient.search(params, pagingParams, None)
    userRes should contain theSameElementsAs (Seq(users(3)))
  }

  test("search by multiple emails") {
    val params = baseSearchParams.copy(emails = Some(Set("funshine.bear@care.alot", "good.luck.bear@care.alot")))
    val (userRes, _) = userClient.search(params, pagingParams, None)
    userRes should contain theSameElementsAs (Seq(users(3), users(4)))
  }

  test("search by singular screen name") {
    val params = baseSearchParams.copy(screenNames = Some(Set("death")))
    val (userRes, _) = userClient.search(params, pagingParams, None)
    userRes should contain theSameElementsAs (Seq(users(0)))
  }

  test("search by multiple screen names") {
    val params = baseSearchParams.copy(screenNames = Some(Set("death", "dark-star")))
    val (userRes, _) = userClient.search(params, pagingParams, None)
    userRes should contain theSameElementsAs (Seq(users(0), users(2)))
  }

  test("search by all the exact match conditions") {
    val params = UserSearchParamSet(
      None,
      Some(Set("good.luck.bear@care.alot")),
      Some(Set("Goodluck Bear")),
      Some(Set("bear")),
      None,
      None
    )
    val (userRes, _) = userClient.search(params, pagingParams, None)
    userRes should contain theSameElementsAs (Seq(users(4)))
  }

  test("search by querying exact name") {
    val params = baseSearchParams.copy(query = Some("death the kid"))
    val (userRes, _) = userClient.search(params, pagingParams, None)
    userRes should contain theSameElementsAs (Seq(users(0), users(1)))
    userRes.head should be(users(1))  // tis the best match
  }

  test("search by quering partial name") {
    val params = baseSearchParams.copy(query = Some("kid"))
    val (userRes, _) = userClient.search(params, pagingParams, None)
    userRes should contain theSameElementsAs (Seq(users(1)))
  }

  test("search by querying exact email") {
    val params = baseSearchParams.copy(query = Some("death.kid@deathcity.com"))
    val (userRes, _) = userClient.search(params, pagingParams, None)
    userRes should contain theSameElementsAs (Seq(users(0), users(1), users(2)))
    userRes.head should be(users(1))  // tis the best match
  }

  test("search by querying email alias") {
    val params = baseSearchParams.copy(query = Some("death.kid"))
    val (userRes, _) = userClient.search(params, pagingParams, None)
    userRes should contain theSameElementsAs (Seq(users(0), users(1)))
    userRes.head should be(users(1))  // tis the best match
  }

  test("search by querying email domain") {
    val params = baseSearchParams.copy(query = Some("care.alot"))
    val (userRes, _) = userClient.search(params, pagingParams, None)
    userRes should contain theSameElementsAs (Seq(users(3), users(4)))
  }

  test("search by all the things") {
    val params = UserSearchParamSet(
      None,
      Some(Set("good.luck.bear@care.alot")),
      Some(Set("Goodluck Bear")),
      Some(Set("bear")),
      None,
      Some("good luck")
    )
    val (userRes, _) = userClient.search(params, pagingParams, None)
    userRes should contain theSameElementsAs (Seq(users(4)))
  }

  test("search by non-existent role, get no results") {
    val params = baseSearchParams.copy(roles = Some(Set("Editor")))
    val (userRes, _) = userClient.search(params, pagingParams, None)
    userRes should be('empty)
  }

  test("search by non-existent domain, get no results") {
    val (userRes, _) = userClient.search(baseSearchParams, pagingParams, Some(8))
    userRes should be('empty)
  }

  test("search by non-existent email, get no results") {
    val params = baseSearchParams.copy(emails = Some(Set("bright.heart.racoon@care.alot")))
    val (userRes, _) = userClient.search(params, pagingParams, None)
    userRes should be('empty)
  }

  test("search by non-existent screen name, get no results") {
    val params =baseSearchParams.copy(screenNames = Some(Set("muffin")))
    val (userRes, _) = userClient.search(params, pagingParams, None)
    userRes should be('empty)
  }

  test("search with paging limits") {
    val params = PagingParamSet(1, 2, None)
    val (userRes, _) = userClient.search(baseSearchParams, params, None)
    userRes.size should be(2)
  }
}
