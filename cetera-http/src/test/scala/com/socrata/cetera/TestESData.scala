package com.socrata.cetera

import java.io.File
import scala.io.Source

import com.rojoma.json.v3.interpolation._
import com.rojoma.json.v3.ast.{JString, JValue}
import com.rojoma.json.v3.io.CompactJsonWriter
import com.rojoma.json.v3.util.JsonUtil
import org.mockserver.integration.ClientAndServer._
import org.mockserver.model.HttpRequest.request
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import com.socrata.cetera.auth.AuthParams
import com.socrata.cetera.metrics.BalboaClient
import com.socrata.cetera.response.{SearchResult, SearchResults}
import com.socrata.cetera.search.{DocumentClient, DomainClient}
import com.socrata.cetera.services.{AutocompleteService, SearchService}
import com.socrata.cetera.types._
import com.socrata.cetera.util.ElasticsearchBootstrap

/*
This trait makes available just about every combination of document/domain/user you might want for testing.
For a summary of the domains that are available, see either test/resources/domains.tsv or look at the DomainSpec
For a summary of the views that are available, see either test/resources/views/ or look at the DocumentSpec
*/
trait TestESData extends TestESDomains with TestESUsers {
  val testSuiteName: String = getClass.getSimpleName.toLowerCase
  val mockServer = startClientAndServer(0)
  val coreTestPort = mockServer.getPort

  val client = new TestESClient(testSuiteName)
  val httpClient = new TestHttpClient()
  val coreClient = new TestCoreClient(httpClient, coreTestPort)

  val domainClient = new DomainClient(client, coreClient, testSuiteName)
  val documentClient = new DocumentClient(client, domainClient, testSuiteName, None, None, Set.empty)
  val balboaDir = new File("/tmp/metrics")
  val balboaClient = new BalboaClient(balboaDir.getAbsolutePath)

  val service = new SearchService(documentClient, domainClient, balboaClient, coreClient)
  val autocompleteService = new AutocompleteService(documentClient, domainClient, coreClient)
  val cookie = "C = Cookie"
  val superAdminBody = j"""{"id" : "who-am-i", "flags" : [ "admin" ]}"""
  val allDomainsParams = Map(
    "domains" -> domains.map(_.domainCname).mkString(","),
    "show_visibility" -> "true"
  ).mapValues(Seq(_))

  val docs = {
    // there is no difference in these two types of docs except for the means by which
    // we used to construct them in commit 1442a6b08.
    val series1DocFiles = (0 to 12).map(i => s"/views/fxf-$i.json")
    val series2DocFiles = (1 to 9).map(i => s"/views/zeta-000$i.json") ++
      (10 to 14).map(i => s"/views/zeta-00$i.json")

    val series1Docs = series1DocFiles.map { f =>
      val source = Source.fromInputStream(getClass.getResourceAsStream(f)).getLines().mkString("\n")
      Document(source).get
    }
    val series2Docs = series2DocFiles.map { f =>
      val source = Source.fromInputStream(getClass.getResourceAsStream(f)).getLines().mkString("\n")
      Document(source).get
    }
    series1Docs.toList ++ series2Docs.toList
  }

  val anonymouslyViewableDocIds =
    List("fxf-0", "fxf-1", "fxf-8", "fxf-10", "fxf-11", "fxf-12", "zeta-0001", "zeta-0002", "zeta-0005", "zeta-0007", "zeta-0012")
  val anonymouslyViewableDocs = docs.filter(d => anonymouslyViewableDocIds contains(d.socrataId.datasetId))

  def bootstrapData(): Unit = {
    ElasticsearchBootstrap.ensureIndex(client, "yyyyMMddHHmm", testSuiteName)

    // load domains
    domains.foreach { d =>
      client.client.prepareIndex(testSuiteName, esDomainType)
        .setSource(JsonUtil.renderJson[Domain](d))
        .setId(d.domainId.toString)
        .setRefresh(true)
        .execute.actionGet
    }

    // load users
    users.foreach { u =>
      client.client.prepareIndex(testSuiteName, esUserType)
        .setSource(JsonUtil.renderJson[EsUser](u))
        .setId(u.id)
        .setRefresh(true)
        .execute.actionGet
    }

    // load data
    docs.foreach { d =>
      client.client.prepareIndex(testSuiteName, esDocumentType)
        .setId(d.socrataId.datasetId)
        .setParent(d.socrataId.domainId.toString)
        .setSource(JsonUtil.renderJson(d))
        .setRefresh(true)
        .execute.actionGet
    }
  }

  def removeBootstrapData(): Unit = {
    client.client.admin().indices().prepareDelete(testSuiteName)
      .execute.actionGet
  }

  def authedUserBodyFromRole(role: String, id: String = "cook-mons"): JValue = {
    j"""{
          "id" : $id,
          "roleName" : $role,
          "rights" : [ "eat_cookies", "spell_words_starting_with_c" ]
        }"""
  }

  def prepareAuthenticatedUser(cookie: String, host: String, userJson: JValue): Unit = {
    val expectedRequest = request()
      .withMethod("GET")
      .withPath("/users.json")
      .withHeader(HeaderXSocrataHostKey, host)
      .withHeader(HeaderCookieKey, cookie)

    // response is hidden by our own "response" directory
    val expectedResponse = org.mockserver.model.HttpResponse.response()
      .withStatusCode(200)
      .withHeader("Content-Type", "application/json; charset=utf-8")
      .withBody(CompactJsonWriter.toString(userJson))

    mockServer
      .when(expectedRequest)
      .respond(expectedResponse)
  }

  def getAllPossibleResults(): Seq[SearchResult] = {
    val host = domains(0).domainCname
    val authedUserBody = j"""{"id" : "who-am-i", "flags" : [ "admin" ]}"""
    val adminCookie = "admin=super"
    prepareAuthenticatedUser(adminCookie, host, authedUserBody)
    service.doSearch(allDomainsParams, requireAuth = true, AuthParams(cookie=Some(adminCookie)), Some(host), None)._2.results
  }

  def fxfs(searchResult: SearchResult): String =
    searchResult.resource.dyn.id.!.asInstanceOf[JString].string

  def fxfs(searchResults: SearchResults[SearchResult]): Seq[String] =
    searchResults.results.map(fxfs)

  def fxfs(docs: Seq[Document]): Seq[String] = docs.map(_.socrataId.datasetId)

  def emptyAndRemoveDir(dir: File): Unit = {
    if (dir.isDirectory) {
      dir.listFiles().foreach(f => f.delete())
    }
    dir.delete()
  }

  def isApproved(res: SearchResult): Boolean = {
    val raApproved = res.metadata.isRoutingApproved.map(identity(_)).getOrElse(true) // None implies not relevant (so approved by default)
    val vmApproved = res.metadata.isModerationApproved.map(identity(_)).getOrElse(true)
    val dlApproved = res.metadata.isDatalensApproved.map(identity(_)).getOrElse(true)
    raApproved && vmApproved && dlApproved
  }
}
