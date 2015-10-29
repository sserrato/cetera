package com.socrata.cetera.search

import com.rojoma.json.v3.ast.JValue
import com.rojoma.json.v3.util.JsonUtil
import com.socrata.cetera._
import com.socrata.cetera.types._
import org.elasticsearch.common.joda.time.DateTime
import org.scalatest.exceptions.TestCanceledException

import scala.io.Source

trait TestESData {
  val client: ElasticSearchClient
  val testSuiteName: String = getClass.getSimpleName.toLowerCase

  private val esDocTemplate: String =
    """{
      | "socrata_id": {
      |   "organization": %s,
      |   "dataset_id": %s,
      |   "domain_cname": [
      |     %s
      |   ],
      |   "site_title": %s,
      |   "domain_id": [
      |     %s
      |   ]
      | },
      | "resource": {
      |   "description": %s,
      |   "nbe_fxf": %s,
      |   "updatedAt": %s,
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
      | "popularity": %s,
      | "is_customer_domain": %s,
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
      | "moderation_status": %s,
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

  private def quoteQualify(s: String): String = "\"%s\"".format(s)
  private def quoteQualify(ss: Seq[String]): String = ss.map(quoteQualify).mkString(",\n")
  private def quoteQualifyScore(sm: Map[String,Float]): String = sm.map { kvp =>
    """{ "name":  "%s", "score": %s } """.format(kvp._1, kvp._2.toString)
  }.mkString(",\n")
  private def quoteQualifyMap(sm: Map[String,String]): String = sm.map { kvp =>
    """{ "key": "%s", "value": "%s" }""".format(kvp._1, kvp._2)
  }.mkString(",\n")

  private def buildEsDoc(socrataIdOrg: String,
                         socrataIdDatasetId: String,
                         socrataIdDomainCnames: Seq[String],
                         siteTitle: String,
                         domainId: Seq[Long],
                         resourceDescription: String,
                         resourceNbeFxf: String,
                         resourceUpdatedAt: String,
                         resourceId: String,
                         resourceColumns: Seq[String],
                         resourceName: String,
                         animlAnnotationsCategories: Map[String,Float],
                         animlAnnotationsTags: Map[String,Float],
                         datatype: String,
                         popularity: Float,
                         isCustomerDomain: Boolean,
                         indexedMetadataName: String,
                         indexedMetadataDescription: String,
                         indexedMetadataColumnsFieldNames: Seq[String],
                         indexedMetadataColumnsDescriptions: Seq[String],
                         indexedMetadataColumnsNames: Seq[String],
                         customerMetadataFlattened: Map[String,String],
                         moderationStatus: String,
                         pageViewsTotal: String,
                         customerCategory: String,
                         customerTags: Seq[String],
                         updateFreq: Long): String =
    esDocTemplate.format(
      socrataIdOrg match {
        case s: String if s.nonEmpty => quoteQualify(s)
        case _ => "null"
      },
      quoteQualify(socrataIdDatasetId),
      quoteQualify(socrataIdDomainCnames),
      quoteQualify(siteTitle),
      domainId.mkString(",\n"),
      quoteQualify(resourceDescription),
      quoteQualify(resourceNbeFxf),
      quoteQualify(resourceUpdatedAt),
      quoteQualify(datatype),
      quoteQualify(resourceId),
      quoteQualify(resourceColumns),
      quoteQualify(resourceName),
      quoteQualifyScore(animlAnnotationsCategories),
      quoteQualifyScore(animlAnnotationsTags),
      quoteQualify(datatype),
      popularity.toString,
      isCustomerDomain.toString,
      quoteQualify(indexedMetadataName),
      quoteQualify(indexedMetadataDescription),
      quoteQualify(indexedMetadataColumnsFieldNames),
      quoteQualify(indexedMetadataColumnsDescriptions),
      quoteQualify(indexedMetadataColumnsNames),
      quoteQualifyMap(customerMetadataFlattened),
      quoteQualify(moderationStatus),
      pageViewsTotal,
      quoteQualify(customerCategory),
      quoteQualify(customerTags),
      updateFreq.toString)

  private def buildEsDocByIndex(i: Int): String = {
    buildEsDoc(
      defaultSocrataIdOrg,
      socrataIdDatasetIds(i % socrataIdDatasetIds.length),
      socrataIdDomainCnames(i % socrataIdDomainCnames.length),
      siteTitles(i % siteTitles.length),
      domainIds(i % domainIds.length),
      resourceDescriptions(i % resourceDescriptions.length),
      resourceNbeFxfs(i % resourceNbeFxfs.length),
      defaultResourceUpdatedAt,
      resourceIds(i % resourceIds.length),
      defaultResourceColumns,
      resourceNames(i % resourceNames.length),
      defaultAaCategories,
      defaultAaTags,
      Datatypes.materialized(i % Datatypes.materialized.length).singular,
      popularities(i % popularities.length),
      isCustomerDomains(i % isCustomerDomains.length),
      imNames(i % imNames.length),
      imDescriptions(i % imDescriptions.length),
      defaultImColumnsFieldNames,
      defaultImColumnsDescriptions,
      defaultImColumnNames,
      domainMetadata(i % domainMetadata.length),
      moderationStatuses(i % moderationStatuses.length),
      pageViewsTotal(i % pageViewsTotal.length),
      domainCategories(i % domainCategories.length),
      domainTags(i % domainTags.length),
      updateFreqs(i % updateFreqs.length))
  }

  val defaultSocrataIdOrg = ""
  val defaultResourceUpdatedAt = DateTime.now().toString
  val defaultResourceColumns = Nil
  val defaultAaCategories = Map("Personal" -> 1F)
  val defaultAaTags = Map("Happy" -> 0.65F, "Accident" -> 0.35F)
  val defaultImColumnsFieldNames = Nil
  val defaultImColumnsDescriptions = Nil
  val defaultImColumnNames = Nil

  val socrataIdDomainCnames = Seq("petercetera.net", "opendata-demo.socrata.com").map(Seq(_))
  val siteTitles = Seq("Temporary URI", "And other things", "Fame and Fortune", "Socrata Demo")
  val domainIds = Seq(1L, 2L, 3L, 4L).map(Seq(_))
  val socrataIdDatasetIds = Seq("aaaa-1111", "aaaa-2222", "aaaa-3333", "aaaa-4444")
  val resourceDescriptions = Seq(
    "We're number one",
    "Second the best",
    "Bird is the third word",
    "May the fourth be with you")
  val resourceNbeFxfs = socrataIdDatasetIds
  val resourceIds = socrataIdDatasetIds
  val resourceNames = Seq("One", "Two", "Three", "Four")
  val popularities = Seq(0.1F, 0.42F, 1F, 42F)
  val isCustomerDomains = Seq(true, true, false)
  val imNames = resourceNames
  val imDescriptions = resourceDescriptions
  val domainMetadata = Seq(Map("one" -> "1"), Map("two" -> "2"), Map.empty[String,String])
  val moderationStatuses = Seq("rejected", "approved", "pending", "irrelevant")
  val pageViewsTotal = Seq.range(1, Datatypes.materialized.length).map(_.toString)
  val domainCategories = Seq("Alpha", "Beta", "Gamma", "")
  val domainTags = Seq("1-one", "2-two", "3-three", "4-four").map(Seq(_))
  val updateFreqs = Seq(1, 2, 3, 4)

  private def indexSettings: String = {
    val s = Source.fromInputStream(getClass.getResourceAsStream("/settings.json")).getLines().mkString("\n")
    val sj = JsonUtil.parseJson[JValue](s) match {
      case Left(e) => throw new TestCanceledException(s"json decode failed: ${e.english}", 0)
      case Right(j) => j
    }
    sj.dyn.settings.!.toString()
  }

  private def datatypeMappings: String = {
    val s = Source.fromInputStream(getClass.getResourceAsStream("/base.json")).getLines().mkString("\n")
    val sj = JsonUtil.parseJson[JValue](s) match {
      case Left(e) => throw new TestCanceledException(s"json decode failed: ${e.english}", 0)
      case Right(j) => j
    }
    sj.dyn.mappings.!.toString()
  }

  def bootstrapSettings(): Unit = {
    client.client.admin().indices().prepareCreate(testSuiteName)
      .setSettings(indexSettings)
      .execute.actionGet
    client.client.admin().indices().preparePutMapping(testSuiteName)
      .setType(DatatypeFieldType.fieldName).setSource(datatypeMappings)
      .setIgnoreConflicts(true)
      .execute.actionGet
    Datatypes.materialized.foreach { datatype =>
      client.client.admin().indices().prepareAliases()
        .addAlias(testSuiteName, datatype.plural)
        .execute.actionGet
    }
  }

  def bootstrapData(): Unit = {
    bootstrapSettings()
    Datatypes.materialized.zipWithIndex.foreach { case (datatype: Materialized, i: Int) =>
      client.client.prepareIndex(testSuiteName, DatatypeFieldType.fieldName)
        .setSource(buildEsDocByIndex(i))
        .setRefresh(true)
        .execute.actionGet
    }
  }

  def removeBootstrapData(): Unit = {
    client.client.admin().indices().prepareDelete(testSuiteName)
      .execute.actionGet
  }
}
