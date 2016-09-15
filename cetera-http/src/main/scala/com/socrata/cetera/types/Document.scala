package com.socrata.cetera.types

import com.rojoma.json.v3.util._
import org.slf4j.LoggerFactory

import com.socrata.cetera.errors.JsonDecodeException

@JsonKeyStrategy(Strategy.Underscore)
case class SocrataId(datasetId: String, parentDatasetid: Option[String], domainId: Int)
object SocrataId { implicit val jCodec = AutomaticJsonCodecBuilder[SocrataId] }

@JsonKeyStrategy(Strategy.Underscore)
case class Resource(description: String,
                    nbeFxf: String,
                    parentFxf: Option[String],
                    updatedAt: String,
                    createdAt: String,
                    @JsonKey("type") datatype: String,
                    id: String,
                    columns: Seq[String],
                    name: String)
object Resource { implicit val jCodec = AutomaticJsonCodecBuilder[Resource] }

@JsonKeyStrategy(Strategy.Underscore)
case class AnimlAnnotations(categories: Seq[String], tags: Seq[String])
object AnimlAnnotations { implicit val jCodec = AutomaticJsonCodecBuilder[AnimlAnnotations] }

@JsonKeyStrategy(Strategy.Underscore)
case class IndexedMetadata(name: String,
                           description: String,
                           columnsFieldName: Seq[String],
                           columnsDescription: Seq[String],
                           columnsName: Seq[String])
object IndexedMetadata { implicit val jCodec = AutomaticJsonCodecBuilder[IndexedMetadata] }

@JsonKeyStrategy(Strategy.Underscore)
case class PageViews(pageViewsTotal: Long,
                     pageViewsLastMonth: Long,
                     pageViewsLastWeek: Long,
                     pageViewsTotalLog: Option[Float] = None,
                     pageViewsLastMonthLog: Option[Float] = None,
                     pageViewsLastWeekLog: Option[Float] = None)
object PageViews { implicit val jCodec = AutomaticJsonCodecBuilder[PageViews] }

@JsonKeyStrategy(Strategy.Underscore)
case class Document(socrataId: SocrataId,
                    resource: Resource,
                    animlAnnotations: AnimlAnnotations,
                    datatype: String,
                    viewtype: String,
                    popularity: Option[Float],
                    indexedMetadata: IndexedMetadata,
                    customerMetadataFlattened: Map[String,String],
                    isDefaultView: Boolean,
                    isModerationApproved: Option[Boolean],
                    approvingDomainIds: Seq[Int],
                    isApprovedByParentDomain: Boolean,
                    pageViews: PageViews,
                    customerCategory: String,
                    customerTags: Seq[String],
                    updatedFreq: Option[Int])
object Document {
  implicit val jCodec = AutomaticJsonCodecBuilder[Document]
  val logger = LoggerFactory.getLogger(getClass)

  def apply(source: String): Option[Document] = {
    Option(source).flatMap { s =>
      JsonUtil.parseJson[Document](s) match {
        case Right(doc) => Some(doc)
        case Left(err) =>
          logger.error(err.english)
          throw new JsonDecodeException(err)
      }
    }
  }
}
