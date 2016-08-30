package com.socrata.cetera.search

import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}

import com.socrata.cetera.handlers.{PagingParamSet, UserSearchParamSet}
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
    val (userRes, totalCount, _) = userClient.search(UserSearchParamSet(), PagingParamSet(), None)
    userRes should contain theSameElementsAs(users)
    totalCount should be(5)
  }

  test("search by singular role") {
    val params = UserSearchParamSet(roles = Some(Set("bear")))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None)
    userRes should contain theSameElementsAs (Seq(users(3), users(4)))
    totalCount should be(2)
  }

  test("search by multiple roles") {
    val params = UserSearchParamSet(roles = Some(Set("bear", "headmaster")))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None)
    userRes should contain theSameElementsAs (Seq(users(1), users(3), users(4)))
    totalCount should be(3)
  }

  test("search by domain") {
    val params = UserSearchParamSet(domain = Some("domain.that.will.be.ignored.com"))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), Some(1))
    userRes should contain theSameElementsAs (Seq(users(3), users(4)))
    totalCount should be(2)
  }

  test("search by singular email") {
    val params = UserSearchParamSet(emails = Some(Set("funshine.bear@care.alot")))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None)
    userRes should contain theSameElementsAs (Seq(users(3)))
    totalCount should be(1)
  }

  test("search by multiple emails") {
    val params = UserSearchParamSet(emails = Some(Set("funshine.bear@care.alot", "good.luck.bear@care.alot")))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None)
    userRes should contain theSameElementsAs (Seq(users(3), users(4)))
    totalCount should be(2)
  }

  test("search by singular screen name") {
    val params = UserSearchParamSet(screenNames = Some(Set("death")))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None)
    userRes should contain theSameElementsAs (Seq(users(0)))
    totalCount should be(1)
  }

  test("search by multiple screen names") {
    val params = UserSearchParamSet(screenNames = Some(Set("death", "dark-star")))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None)
    userRes should contain theSameElementsAs (Seq(users(0), users(2)))
    totalCount should be(2)
  }

  test("search by singular flag") {
    val params = UserSearchParamSet(flags = Some(Set("symmetrical")))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None)
    userRes should contain theSameElementsAs (Seq(users(0)))
    totalCount should be(1)
  }

  test("search by multiple flags") {
    val params = UserSearchParamSet(flags = Some(Set("yellow", "green")))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None)
    userRes should contain theSameElementsAs (Seq(users(3), users(4)))
    totalCount should be(2)
  }

  test("search by all the exact match conditions") {
    val params = UserSearchParamSet(
      emails = Some(Set("good.luck.bear@care.alot")),
      screenNames = Some(Set("Goodluck Bear")),
      roles = Some(Set("bear"))
    )
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None)
    userRes should contain theSameElementsAs (Seq(users(4)))
    totalCount should be(1)
  }

  test("search by querying exact name") {
    val params = UserSearchParamSet(query = Some("death the kid"))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None)
    userRes should contain theSameElementsAs (Seq(users(0), users(1)))
    userRes.head should be(users(1))  // tis the best match
    totalCount should be(2)
  }

  test("search by quering partial name") {
    val params = UserSearchParamSet(query = Some("kid"))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None)
    userRes should contain theSameElementsAs (Seq(users(1)))
    totalCount should be(1)
  }

  test("search by querying exact email") {
    val params = UserSearchParamSet(query = Some("death.kid@deathcity.com"))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None)
    userRes should contain theSameElementsAs (Seq(users(0), users(1), users(2)))
    userRes.head should be(users(1))  // tis the best match
    totalCount should be(3)
  }

  test("search by querying email alias") {
    val params = UserSearchParamSet(query = Some("death.kid"))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None)
    userRes should contain theSameElementsAs (Seq(users(0), users(1)))
    userRes.head should be(users(1))  // tis the best match
    totalCount should be(2)
  }

  test("search by querying email domain") {
    val params = UserSearchParamSet(query = Some("care.alot"))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None)
    userRes should contain theSameElementsAs (Seq(users(3), users(4)))
    totalCount should be(2)
  }

  test("search by all the things") {
    val params = UserSearchParamSet(
      emails = Some(Set("good.luck.bear@care.alot")),
      screenNames = Some(Set("Goodluck Bear")),
      roles = Some(Set("bear")),
      query = Some("good luck")
    )
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None)
    userRes should contain theSameElementsAs (Seq(users(4)))
    totalCount should be(1)
  }

  test("search by non-existent role, get no results") {
    val params = UserSearchParamSet(roles = Some(Set("Editor")))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None)
    userRes should be('empty)
    totalCount should be(0)
  }

  test("search by non-existent domain, get no results") {
    val (userRes, totalCount, _) = userClient.search(UserSearchParamSet(), PagingParamSet(), Some(80))
    userRes should be('empty)
    totalCount should be(0)
  }

  test("search by non-existent email, get no results") {
    val params = UserSearchParamSet(emails = Some(Set("bright.heart.racoon@care.alot")))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None)
    userRes should be('empty)
    totalCount should be(0)
  }

  test("search by non-existent screen name, get no results") {
    val params = UserSearchParamSet(screenNames = Some(Set("muffin")))
    val (userRes, totalCount, _) = userClient.search(params, PagingParamSet(), None)
    userRes should be('empty)
    totalCount should be(0)
  }

  test("search with paging limits") {
    val params = PagingParamSet(offset = 1, limit = 2)
    val (userRes, totalCount, _) = userClient.search(UserSearchParamSet(), params, None)
    userRes.size should be(2)
    totalCount should be(5)
  }
}
