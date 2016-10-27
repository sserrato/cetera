package com.socrata.cetera.types

import com.rojoma.json.v3.util._
import org.slf4j.LoggerFactory

import com.socrata.cetera.errors.JsonDecodeException

@JsonKeyStrategy(Strategy.Underscore)
case class SocrataId(
    datasetId: String,
    nbeId: Option[String],
    obeId: Option[String],
    parentDatasetId: Option[Set[String]],
    domainId: BigInt)

object SocrataId {
  implicit val jCodec = AutomaticJsonCodecBuilder[SocrataId]
}

@JsonKeyStrategy(Strategy.Underscore)
case class Resource(
    description: String,
    nbeFxf: String,
    parentFxf: Option[String],
    updatedAt: String,
    createdAt: String,
    @JsonKey("type") datatype: String,
    id: String,
    columns: Seq[String],
    name: String,
    attribution: Option[String],
    provenance: Option[String]
)
object Resource { implicit val jCodec = AutomaticJsonCodecBuilder[Resource] }

@JsonKeyStrategy(Strategy.Underscore)
@NullForNone
case class Annotation(name: String, score: BigDecimal)

object Annotation {
  implicit val jCodec = AutomaticJsonCodecBuilder[Annotation]
}

@JsonKeyStrategy(Strategy.Underscore)
case class AnimlAnnotations(categories: Seq[Annotation], tags: Seq[Annotation])
object AnimlAnnotations { implicit val jCodec = AutomaticJsonCodecBuilder[AnimlAnnotations] }

@JsonKeyStrategy(Strategy.Underscore)
case class IndexedMetadata(
    name: String,
    description: String,
    columnsFieldName: Seq[String],
    columnsDescription: Seq[String],
    columnsName: Seq[String])

object IndexedMetadata {
  implicit val jCodec = AutomaticJsonCodecBuilder[IndexedMetadata]
}

@JsonKeyStrategy(Strategy.Underscore)
case class PageViews(pageViewsTotal: Long,
                     pageViewsLastMonth: Long,
                     pageViewsLastWeek: Long,
                     pageViewsTotalLog: Option[Float] = None,
                     pageViewsLastMonthLog: Option[Float] = None,
                     pageViewsLastWeekLog: Option[Float] = None)
object PageViews { implicit val jCodec = AutomaticJsonCodecBuilder[PageViews] }

@JsonKeyStrategy(Strategy.Underscore)
case class ESGrant(
    @JsonKey("type") grantType: String,
    userId: String)

object ESGrant {
  implicit val jCodec = AutomaticJsonCodecBuilder[ESGrant]
}

@JsonKeyStrategy(Strategy.Underscore)
case class CustomerMetadataFlattened(key: String, value: String)

object CustomerMetadataFlattened {
  implicit val jCodec = AutomaticJsonCodecBuilder[CustomerMetadataFlattened]
}

@JsonKeyStrategy(Strategy.Underscore)
case class Document(
    pageViews: Option[PageViews], // with Some for log variants
    datatype: String,
    viewtype: String,
    isModerationApproved: Option[Boolean],
    isDefaultView: Boolean,
    isPublic: Boolean,
    isPublished: Boolean,
    popularity: BigDecimal,
    updateFreq: BigDecimal,
    indexedMetadata: IndexedMetadata,
    resource: Resource,
    socrataId: SocrataId,
    animlAnnotations: Option[AnimlAnnotations],
    approvingDomainIds: Option[Seq[Int]],
    isApprovedByParentDomain: Boolean,
    customerTags: Seq[String],
    customerCategory: String,
    // If this comes back empty, it comes back as an empty object, not an empty array
    customerMetadataFlattened: Seq[CustomerMetadataFlattened],
    createdAt: String,
    updatedAt: String,
    indexedAt: Option[String],
    ownerId: String,
    sharedTo: Seq[String],
    attribution: Option[String],
    provenance: Option[String],
    previewImageId: Option[String],
    grants: Seq[ESGrant],
    hideFromCatalog: Option[Boolean],
    hideFromDataJson: Option[Boolean])

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
