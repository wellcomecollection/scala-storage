package uk.ac.wellcome.storage.fixtures

import com.gu.scanamo.{DynamoFormat, Scanamo}
import com.gu.scanamo.syntax._
import io.circe.Encoder
import org.scalatest.Matchers
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.json.utils.JsonAssertions
import uk.ac.wellcome.storage.ObjectStore
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table
import uk.ac.wellcome.storage.fixtures.S3.Bucket
import uk.ac.wellcome.storage.vhs.{HybridRecord, VHSConfig, VersionedHybridStore}

import scala.concurrent.ExecutionContext.Implicits.global

trait LocalVersionedHybridStore
    extends LocalDynamoDbVersioned
    with S3
    with JsonAssertions
    with Matchers {

  val defaultGlobalS3Prefix = "testing"

  def vhsLocalFlags(bucket: Bucket,
                    table: Table,
                    globalS3Prefix: String = defaultGlobalS3Prefix) =
    Map(
      "aws.vhs.s3.bucketName" -> bucket.name,
      "aws.vhs.s3.globalPrefix" -> globalS3Prefix,
      "aws.vhs.dynamo.tableName" -> table.name
    ) ++ s3ClientLocalFlags ++ dynamoClientLocalFlags

  def withTypeVHS[T, Metadata, R](bucket: Bucket,
                                  table: Table,
                                  globalS3Prefix: String = defaultGlobalS3Prefix)(
    testWith: TestWith[VersionedHybridStore[T, Metadata, ObjectStore[T]], R])(
    implicit objectStore: ObjectStore[T]
  ): R = {
    val vhsConfig = createVHSConfigWith(
      table = table,
      bucket = bucket,
      globalS3Prefix = globalS3Prefix
    )

    val store = new VersionedHybridStore[T, Metadata, ObjectStore[T]](
      vhsConfig = vhsConfig,
      objectStore = objectStore,
      dynamoDbClient = dynamoDbClient
    )
    testWith(store)
  }

  def assertStored[T](bucket: Bucket, table: Table, id: String, record: T)(
    implicit encoder: Encoder[T]) =
    assertJsonStringsAreEqual(
      getJsonFor(table, id),
      toJson(record).get
    )

  def getJsonFor(table: Table, id: String): String = {
    val hybridRecord = getHybridRecord(table, id)

    getJsonFromS3(hybridRecord.location).noSpaces
  }

  def getContentFor(table: Table, id: String): String = {
    val hybridRecord = getHybridRecord(table, id)

    getContentFromS3(hybridRecord.location)
  }

  def assertStoredCorrectly[T](expectedHybridRecord: HybridRecord, expectedContents: T, bucket: Bucket, table: Table) = {
    val hybridRecord = getHybridRecord(table, expectedHybridRecord.id)
    hybridRecord shouldBe expectedHybridRecord
    getContentFromS3(hybridRecord.location) shouldBe expectedContents
  }

  protected def getHybridRecord(table: Table, id: String): HybridRecord =
    Scanamo.get[HybridRecord](dynamoDbClient)(table.name)('id -> id) match {
      case None => throw new RuntimeException(s"No object with id $id found!")
      case Some(read) =>
        read match {
          case Left(error) =>
            throw new RuntimeException(s"Error reading from dynamo: $error")
          case Right(record) => record
        }
    }

  protected def getRecordMetadata[T](table: Table, id: String)(
    implicit dynamoFormat: DynamoFormat[T]) =
    Scanamo.get[T](dynamoDbClient)(table.name)('id -> id) match {
      case None => throw new RuntimeException(s"No object with id $id found!")
      case Some(read) =>
        read match {
          case Left(error) =>
            throw new RuntimeException(s"Error reading from dynamo: $error")
          case Right(record) => record
        }
    }

  def createVHSConfigWith(
    table: Table,
    bucket: Bucket,
    globalS3Prefix: String
  ): VHSConfig =
    VHSConfig(
      dynamoConfig = createDynamoConfigWith(table),
      s3Config = createS3ConfigWith(bucket),
      globalS3Prefix = globalS3Prefix
    )
}
