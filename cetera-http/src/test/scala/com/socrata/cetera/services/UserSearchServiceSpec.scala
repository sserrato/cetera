package com.socrata.cetera.services

import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}

import com.socrata.cetera.search.UserClient
import com.socrata.cetera.{TestESClient, TestESData}

class UserSearchServiceSpec extends FunSuiteLike with Matchers with TestESData with BeforeAndAfterAll {
  val client = new TestESClient(testSuiteName)
  val userClient = new UserClient(client, testSuiteName)
  val service = new UserSearchService(userClient)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    bootstrapData()
  }

  override protected def afterAll(): Unit = {
    removeBootstrapData()
    client.close()
    super.afterAll()
  }

  test("search without authentication for any and all users yields results and timing") {
    val (results, timings) = service.doSearch(Map.empty, None, None, None)

    results.results.headOption should be('defined)
    timings.searchMillis.headOption should be('defined)
  }
}
