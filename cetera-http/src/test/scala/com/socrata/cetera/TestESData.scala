package com.socrata.cetera

import scala.io.Source

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.util.JsonUtil
import org.elasticsearch.common.joda.time.DateTime
import org.scalatest.exceptions.TestCanceledException

import com.socrata.cetera.search.ElasticSearchClient
import com.socrata.cetera.types._
import com.socrata.cetera.util.ElasticsearchBootstrap

trait TestESData {
  val client: ElasticSearchClient
  val testSuiteName: String = getClass.getSimpleName.toLowerCase

  private val esDocTemplate: String =
    """{
      | "socrata_id": {
      |   "dataset_id": %s,
      |   "domain_id": %s
      | },
      | "resource": {
      |   "description": %s,
      |   "nbe_fxf": %s,
      |   "updatedAt": %s,
      |   "createdAt": %s,
      |   "type": %s,
      |   "id": %s,
      |   "columns": {
      |     %s
      |   },
      |   "name": %s
      | },
      | "animl_annotations": {
      |   "categories": [
      |     %s
      |   ],
      |   "tags": [
      |     %s
      |   ]
      | },
      | "datatype": %s,
      | "viewtype": %s,
      | "popularity": %s,
      | "indexed_metadata": {
      |   "name": %s,
      |   "description": %s,
      |   "columns_field_name": [
      |     %s
      |   ],
      |   "columns_description": [
      |     %s
      |   ],
      |   "columns_name": [
      |     %s
      |   ]
      | },
      | "customer_metadata_flattened": [
      |   %s
      | ],
      | "is_public": %s,
      | "is_default_view": %s,
      | "is_moderation_approved": %s,
      | "approving_domain_ids": [
      |   %s
      | ],
      | "is_approved_by_parent_domain": %s,
      | "page_views": {
      |   "page_views_total": %s
      | },
      | "customer_category": %s,
      | "customer_tags": [
      |   %s
      | ],
      | "update_freq": %s
      |}
    """.stripMargin

  val esDomainTemplate: String =
    """{
      | "domain_cname": %s,
      | "domain_id": %s,
      | "site_title": %s,
      | "organization": %s,
      | "is_customer_domain": %s,
      | "moderation_enabled": %s,
      | "routing_approval_enabled": %s,
      | "locked_down": %s,
      | "api_locked_down": %s
      |}
    """.stripMargin

  private def quoteQualify(s: String): String = "\"%s\"".format(s)
  private def quoteQualify(ss: Seq[String]): String = ss.map(quoteQualify).mkString(",\n")
  private def quoteQualifyScore(sm: Map[String,Float]): String = sm.map { kvp =>
    """{ "name":  "%s", "score": %s } """.format(kvp._1, kvp._2.toString)
  }.mkString(",\n")
  private def quoteQualifyMap(sm: Map[String,String]): String = sm.map { kvp =>
    """{ "key": "%s", "value": "%s" }""".format(kvp._1, kvp._2)
  }.mkString(",\n")

  private def buildEsDoc(socrataIdDatasetId: String,
                         domainId: Int,
                         resourceDescription: String,
                         resourceNbeFxf: String,
                         resourceUpdatedAt: String,
                         resourceCreatedAt: String,
                         resourceId: String,
                         resourceColumns: Seq[String],
                         resourceName: String,
                         animlAnnotationsCategories: Map[String,Float],
                         animlAnnotationsTags: Map[String,Float],
                         datatype: String,
                         viewtype: String,
                         popularity: Float,
                         indexedMetadataName: String,
                         indexedMetadataDescription: String,
                         indexedMetadataColumnsFieldNames: Seq[String],
                         indexedMetadataColumnsDescriptions: Seq[String],
                         indexedMetadataColumnsNames: Seq[String],
                         customerMetadataFlattened: Map[String,String],
                         isPublic: Boolean,
                         isDefaultView: Boolean,
                         isModerationApproved: Option[Boolean],
                         approvingDomainIds: Seq[Int],
                         isApprovedByParentDomain: Boolean,
                         pageViewsTotal: String,
                         customerCategory: String,
                         customerTags: Seq[String],
                         updateFreq: Long): String = {
    val doc = esDocTemplate.format(
      quoteQualify(socrataIdDatasetId),
      domainId.toString,
      quoteQualify(resourceDescription),
      quoteQualify(resourceNbeFxf),
      quoteQualify(resourceUpdatedAt),
      quoteQualify(resourceCreatedAt),
      quoteQualify(datatype),
      quoteQualify(resourceId),
      quoteQualify(resourceColumns),
      quoteQualify(resourceName),
      quoteQualifyScore(animlAnnotationsCategories),
      quoteQualifyScore(animlAnnotationsTags),
      quoteQualify(datatype),
      quoteQualify(viewtype),
      popularity.toString,
      quoteQualify(indexedMetadataName),
      quoteQualify(indexedMetadataDescription),
      quoteQualify(indexedMetadataColumnsFieldNames),
      quoteQualify(indexedMetadataColumnsDescriptions),
      quoteQualify(indexedMetadataColumnsNames),
      quoteQualifyMap(customerMetadataFlattened),
      isPublic.toString,
      isDefaultView.toString,
      isModerationApproved.map(_.toString).getOrElse("null"),
      approvingDomainIds.mkString(","),
      isApprovedByParentDomain.toString,
      pageViewsTotal,
      quoteQualify(customerCategory),
      quoteQualify(customerTags),
      updateFreq.toString)
    doc
  }

  private def buildEsDomain(domainCname: String, domainId: Int, siteTitle: String, organization: String,
                            isCustomerDomain: Boolean, moderationEnabled: Boolean, routingApprovalEnabled: Boolean,
                            lockedDown: Boolean, apiLockedDown: Boolean) =
    esDomainTemplate.format(quoteQualify(domainCname), domainId.toString, quoteQualify(siteTitle), quoteQualify(organization),
                            isCustomerDomain.toString, moderationEnabled.toString, routingApprovalEnabled.toString,
                            lockedDown.toString, apiLockedDown.toString)

  private def buildEsDocByIndex(i: Int): String = {
    val domainId = i % domainCnames.length
    val domainApprovalIds = approvingDomainIds(i % approvingDomainIds.length)
    buildEsDoc(
      socrataIdDatasetIds(i % socrataIdDatasetIds.length),
      domainId,
      resourceDescriptions(i % resourceDescriptions.length),
      resourceNbeFxfs(i % resourceNbeFxfs.length),
      defaultResourceUpdatedAt,
      defaultResourceCreatedAt,
      resourceIds(i % resourceIds.length),
      defaultResourceColumns,
      resourceNames(i % resourceNames.length),
      defaultAaCategories,
      defaultAaTags,
      Datatypes.materialized(i % Datatypes.materialized.length).singular,
      "",
      popularities(i % popularities.length),
      imNames(i % imNames.length),
      imDescriptions(i % imDescriptions.length),
      defaultImColumnsFieldNames,
      defaultImColumnsDescriptions,
      defaultImColumnNames,
      domainMetadata(i % domainMetadata.length),
      isPublics(i % isPublics.length),
      isDefaultViews(i % isDefaultViews.length),
      isModerationApproveds(i % isModerationApproveds.length),
      domainApprovalIds,
      domainApprovalIds.contains(domainId),
      pageViewsTotal(i % pageViewsTotal.length),
      domainCategories(i % domainCategories.length),
      domainTags(i % domainTags.length),
      updateFreqs(i % updateFreqs.length))
  }

  private def buildEsDomainByIndex(cname:String, i: Int): String = {
    buildEsDomain(cname,
                  i,
                  siteTitles(i % siteTitles.length),
                  defaultSocrataIdOrg,
                  isCustomerDomains(i % isCustomerDomains.length),
                  isDomainModerated(i % isDomainModerated.length),
                  hasRoutingApproval(i % hasRoutingApproval.length),
                  false,
                  false)
  }

  val domainCnames = Seq("petercetera.net", "opendata-demo.socrata.com", "blue.org", "annabelle.island.net")
  val siteTitles = Seq("Temporary URI", "And other things", "Fame and Fortune", "Socrata Demo")
  val defaultSocrataIdOrg = ""
  val isCustomerDomains = Seq(true, false, true, true)
  val isDomainModerated = Seq(false, true, false, true)
  val hasRoutingApproval = Seq(false, false, true, true)

  val defaultResourceUpdatedAt = DateTime.now().toString
  val defaultResourceCreatedAt = DateTime.now().toString
  val defaultResourceColumns = Nil
  val defaultAaCategories = Map("Personal" -> 1F)
  val defaultAaTags = Map("Happy" -> 0.65F, "Accident" -> 0.35F)
  val defaultImColumnsFieldNames = Nil
  val defaultImColumnsDescriptions = Nil
  val defaultImColumnNames = Nil

  val numericIds = 0 to Datatypes.materialized.length
  val socrataIdDatasetIds = numericIds.map(n => s"fxf-$n")
  val resourceDescriptions = Seq(
    "We're number one",
    "Second the best",
    "Bird is the third word",
    "May the fourth be with you")
  val resourceNbeFxfs = socrataIdDatasetIds
  val resourceIds = socrataIdDatasetIds
  val resourceNames = Seq("One", "Two", "Three", "Four")
  val popularities = Seq(0.1F, 0.42F, 1F, 42F)
  val imNames = resourceNames
  val imDescriptions = resourceDescriptions
  val domainMetadata = Seq(Map("one" -> "1", "two" -> "3", "five" -> "8"), Map("one" -> "2"), Map("two" -> "3"), Map.empty[String,String])
  val isModerationApproveds = Seq(Some(false), Some(true), None, None, None)
  val isPublics = Seq(true)
  val isDefaultViews = Seq(false, false, false, true, false)
  val approvingDomains = Seq("petercetera.net", "blue.org", "annabelle.island.demo").map(Seq(_))
  val approvingDomainIds = Seq(Seq(0), Seq(2), Seq(3))
  val pageViewsTotal = Seq.range(1, Datatypes.materialized.length).map(_.toString)
  val domainCategories = Seq("Alpha", "Beta", "Gamma", "")
  val domainTags = Seq("1-one", "2-two", "3-three", "4-four").map(Seq(_))
  val updateFreqs = Seq(1, 2, 3, 4)

  private def indexSettings: String = {
    val s = Source.fromInputStream(getClass.getResourceAsStream("/esSettings.json")).getLines().mkString("\n")
    val sj = JsonUtil.parseJson[JValue](s) match {
      case Left(e) => throw new TestCanceledException(s"json decode failed: ${e.english}", 0)
      case Right(j) => j
    }
    sj.dyn.settings.!.toString()
  }

  private def datatypeMappings(datatype: String): String = {
    val s = Source.fromInputStream(getClass.getResourceAsStream("/esMappings.json")).getLines().mkString("\n")
    val sj = JsonUtil.parseJson[JValue](s) match {
      case Left(e) => throw new TestCanceledException(s"json decode failed: ${e.english}", 0)
      case Right(j) => j
    }
    val mappings = sj.dyn.mappings
    val typeMapping = datatype match {
      case s: String if s == "domain" => mappings.domain
      case s: String if s == "document" => mappings.document
    }
    typeMapping.!.toString()
  }

  def bootstrapData(): Unit = {
    ElasticsearchBootstrap.ensureIndex(client, "yyyyMMddHHmm", testSuiteName)

    val additionalCnames = Seq("dylan.demo.socrata.com", "dylan2.demo.socrata.com")

    (domainCnames ++ additionalCnames).zipWithIndex.foreach { case (cname: String, i: Int) =>
      client.client.prepareIndex(testSuiteName, esDomainType)
        .setSource(buildEsDomainByIndex(cname, i))
        .setId(i.toString)
        .setRefresh(true)
        .execute.actionGet
    }
    Datatypes.materialized.zipWithIndex.foreach { case (datatype: Materialized, i: Int) =>
      client.client.prepareIndex(testSuiteName, esDocumentType)
        .setParent((i % domainCnames.length).toString)
        .setSource(buildEsDocByIndex(i))
        .setRefresh(true)
        .execute.actionGet
    }
    // TODO: more refined test document creation
    Seq(
      (0, buildEsDoc(
        "zeta-0001", 0,
        "a stale moderation status", "zeta-0001", DateTime.now.toString, DateTime.now.toString,
        "zeta-0001", Seq.empty, "", Map.empty, Map.empty,
        TypeDatasets.singular, viewtype = "", 0F,
        "", "", Seq.empty, Seq.empty, Seq.empty,
        Map.empty,
        isPublic = true, isDefaultView = false, Some(true), Seq(0), isApprovedByParentDomain = true,
        "42", "Fun", Seq.empty, 0L
      )),
      (3, buildEsDoc(
        "zeta-0002", 3,
        "full routing & approval", "zeta-0002", DateTime.now.toString, DateTime.now.toString,
        "zeta-0002", Seq.empty, "", Map.empty, Map.empty,
        TypeDatasets.singular, viewtype = "", 0F,
        "", "", Seq.empty, Seq.empty, Seq.empty,
        Map.empty,
        isPublic = true, isDefaultView = false, Some(true), Seq(2,3), isApprovedByParentDomain = true,
        "42", "Fun", Seq.empty, 0L
      )),
      (0, buildEsDoc(
        "zeta-0003", 0,
        "private dataset", "zeta-0003", DateTime.now.toString, DateTime.now.toString,
        "zeta-0003", Seq.empty, "", Map.empty, Map.empty,
        TypeDatasets.singular, viewtype = "", 0F,
        "", "", Seq.empty, Seq.empty, Seq.empty,
        Map.empty,
        isPublic = false, isDefaultView = true, Some(true), Seq(0, 1, 2,3), isApprovedByParentDomain = true,
        "42", "Private", Seq.empty, 0L
      )),
      (0, buildEsDoc(
        "zeta-0004", 0,
        "standalone visualization", "zeta-0004", DateTime.now.toString, DateTime.now.toString,
        "zeta-0004", Seq.empty, "", Map.empty, Map.empty,
        TypeDatalensCharts.singular, viewtype = "", 0F,
        "", "", Seq.empty, Seq.empty, Seq.empty,
        Map.empty,
        isPublic = true, isDefaultView = false, None, Seq(0, 1, 2,3), isApprovedByParentDomain = true,
        "42", "Standalone", Seq.empty, 0L
      ))
    ).foreach { case (domain, doc) =>
      client.client.prepareIndex(testSuiteName, esDocumentType)
        .setParent(domain.toString)
        .setSource(doc)
        .setRefresh(true)
        .execute.actionGet
    }
  }

  def removeBootstrapData(): Unit = {
    client.client.admin().indices().prepareDelete(testSuiteName)
      .execute.actionGet
  }
}
