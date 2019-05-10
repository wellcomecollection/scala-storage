package uk.ac.wellcome.storage.vhs

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.gu.scanamo.DynamoFormat
import grizzled.slf4j.Logging
import uk.ac.wellcome.storage.dynamo.{
  DynamoVersionedDao,
  UpdateExpressionGenerator
}
import uk.ac.wellcome.storage.type_classes.Migration._
import uk.ac.wellcome.storage.type_classes.{
  HybridRecordEnricher,
  IdGetter,
  VersionGetter,
  VersionUpdater,
  _
}
import uk.ac.wellcome.storage.{KeyPrefix, ObjectLocation, ObjectStore}

import scala.concurrent.{ExecutionContext, Future}

case class EmptyMetadata()

class VersionedHybridStore[T, Metadata, Store <: ObjectStore[T]](
  vhsConfig: VHSConfig,
  objectStore: Store,
  dynamoDbClient: AmazonDynamoDB
)(implicit ec: ExecutionContext)
    extends Logging {

  def versionedDao[DynamoRow](
    implicit
    dynamoFormat: DynamoFormat[DynamoRow],
    versionUpdater: VersionUpdater[DynamoRow],
    idGetter: IdGetter[DynamoRow],
    versionGetter: VersionGetter[DynamoRow],
    updateExpressionGenerator: UpdateExpressionGenerator[DynamoRow])
    : DynamoVersionedDao[DynamoRow] =
    new DynamoVersionedDao[DynamoRow](
      dynamoDbClient = dynamoDbClient,
      dynamoConfig = vhsConfig.dynamoConfig
    )

  private case class VersionedHybridObject(
    vhsIndexEntry: VHSIndexEntry[Metadata],
    s3Object: T
  )

  // Store a single record in DynamoDB.
  //
  // You pass it a record and optionally a case class containing some metadata.
  // The HybridRecordEnricher combines this with the HybridRecord, and stores
  // both of them as a single row in DynamoDB.
  //
  // Update and version logic (e.g., do not store this record if a newer record already exists)
  // is handled through the mappings ifNotExisting and ifExisting
  //
  def updateRecord[DynamoRow](id: String)(ifNotExisting: => (T, Metadata))(
    ifExisting: (T, Metadata) => (T, Metadata))(
    implicit enricher: HybridRecordEnricher.Aux[Metadata, DynamoRow],
    dynamoFormat: DynamoFormat[DynamoRow],
    versionUpdater: VersionUpdater[DynamoRow],
    idGetter: IdGetter[DynamoRow],
    versionGetter: VersionGetter[DynamoRow],
    updateExpressionGenerator: UpdateExpressionGenerator[DynamoRow],
    migrationH: Migration[DynamoRow, HybridRecord],
    migrationM: Migration[DynamoRow, Metadata]
  ): Future[VHSIndexEntry[Metadata]] =
    getObject[DynamoRow](id).flatMap {
      case Some(
          VersionedHybridObject(
            VHSIndexEntry(storedHybridRecord, storedMetadata),
            storedS3Record
          )
          ) =>
        debug(s"Existing object $id")
        val (transformedS3Record, transformedMetadata) =
          ifExisting(storedS3Record, storedMetadata)

        if (transformedS3Record != storedS3Record || transformedMetadata != storedMetadata) {
          debug("existing object changed, updating")
          putObject(
            id,
            transformedS3Record,
            enricher
              .enrichedHybridRecordHList(
                id = id,
                metadata = transformedMetadata,
                version = storedHybridRecord.version)
          ).map { hybridRecord =>
            VHSIndexEntry(
              hybridRecord = hybridRecord,
              metadata = transformedMetadata
            )
          }
        } else {
          debug("existing object unchanged, not updating")
          Future.successful(
            VHSIndexEntry(
              hybridRecord = storedHybridRecord,
              metadata = storedMetadata
            )
          )
        }
      case None =>
        debug("NotExisting object")
        val (s3Record, metadata) = ifNotExisting
        debug(s"creating $id")
        putObject(
          id = id,
          s3Record,
          enricher.enrichedHybridRecordHList(
            id = id,
            metadata = metadata,
            version = 0
          )
        ).map { hybridRecord =>
          VHSIndexEntry(
            hybridRecord = hybridRecord,
            metadata = metadata
          )
        }
    }

  def getRecord[DynamoRow](id: String)(
    implicit enricher: HybridRecordEnricher.Aux[Metadata, DynamoRow],
    dynamoFormat: DynamoFormat[DynamoRow],
    versionUpdater: VersionUpdater[DynamoRow],
    idGetter: IdGetter[DynamoRow],
    versionGetter: VersionGetter[DynamoRow],
    updateExpressionGenerator: UpdateExpressionGenerator[DynamoRow],
    migrationH: Migration[DynamoRow, HybridRecord],
    migrationM: Migration[DynamoRow, Metadata]
  ): Future[Option[T]] = {

    // The compiler wrongly thinks `enricher` is unused.
    // This no-op persuades it to ignore it.
    identity(enricher)

    getObject[DynamoRow](id).map { maybeObject =>
      maybeObject.map(_.s3Object)
    }
  }

  private def putObject[DynamoRow](id: String,
                                   t: T,
                                   f: ObjectLocation => DynamoRow)(
    implicit dynamoFormat: DynamoFormat[DynamoRow],
    versionUpdater: VersionUpdater[DynamoRow],
    idGetter: IdGetter[DynamoRow],
    versionGetter: VersionGetter[DynamoRow],
    migrationH: Migration[DynamoRow, HybridRecord],
    updateExpressionGenerator: UpdateExpressionGenerator[DynamoRow]
  ): Future[HybridRecord] =
    for {
      objectLocation <- Future.fromTry {
        objectStore.put(vhsConfig.s3Config.bucketName)(
          t,
          keyPrefix = KeyPrefix(buildKeyPrefix(id))
        )
      }
      dynamoRow <- Future.fromTry {
        versionedDao[DynamoRow].put(f(objectLocation))
      }
    } yield dynamoRow.migrateTo[HybridRecord]

  // To spread objects evenly in our S3 bucket, we take the last two
  // characters of the ID and reverse them.  This ensures that:
  //
  //  1.  We can find the S3 data corresponding to a given source ID
  //      without consulting the index.
  //
  //  2.  We can identify which record an S3 object is associated with.
  //
  //  3.  Adjacent objects are stored in shards that are far apart,
  //      e.g. 0001 and 0002 are separated by nine shards.
  //
  private def buildKeyPrefix(id: String): String =
    s"${vhsConfig.globalS3Prefix.stripSuffix("/")}/$id"

  private def getObject[DynamoRow](id: String)(
    implicit
    dynamoFormat: DynamoFormat[DynamoRow],
    versionUpdater: VersionUpdater[DynamoRow],
    idGetter: IdGetter[DynamoRow],
    versionGetter: VersionGetter[DynamoRow],
    updateExpressionGenerator: UpdateExpressionGenerator[DynamoRow],
    migrationH: Migration[DynamoRow, HybridRecord],
    migrationM: Migration[DynamoRow, Metadata]
  ): Future[Option[VersionedHybridObject]] = {

    val dynamoRecord: Future[Option[DynamoRow]] = Future.fromTry {
      versionedDao[DynamoRow].get(id = id)
    }

    dynamoRecord.flatMap {
      case Some(dynamoRow) => {

        val hybridRecord = dynamoRow.migrateTo[HybridRecord]
        val metadata = dynamoRow.migrateTo[Metadata]

        val vhsIndexEntry = VHSIndexEntry(
          hybridRecord = hybridRecord,
          metadata = metadata
        )

        Future
          .fromTry {
            objectStore
              .get(hybridRecord.location)
          }
          .map { s3Object =>
            Some(
              VersionedHybridObject(
                vhsIndexEntry = vhsIndexEntry,
                s3Object = s3Object))
          }
      }
      case None => Future.successful(None)
    }
  }
}
