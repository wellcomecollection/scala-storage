package uk.ac.wellcome.storage.fixtures

import com.gu.scanamo.{DynamoFormat, Scanamo}
import com.gu.scanamo.syntax._
import org.scalatest.Matchers
import uk.ac.wellcome.storage.ObjectStore
import uk.ac.wellcome.storage.dynamo.DynamoConfig
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table
import uk.ac.wellcome.storage.fixtures.S3.Bucket
import uk.ac.wellcome.storage.s3._
import uk.ac.wellcome.storage.utils.JsonUtil
import uk.ac.wellcome.storage.vhs.{HybridRecord, VHSConfig, VersionedHybridStore}

import scala.concurrent.ExecutionContext.Implicits.global

trait LocalVersionedHybridStore
    extends LocalDynamoDbVersioned
    with S3
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
                                        globalS3Prefix: String =
                                          defaultGlobalS3Prefix)(
    testWith: TestWith[VersionedHybridStore[T, Metadata, ObjectStore[T]], R])(
    implicit objectStore: ObjectStore[T]
  ): R = {

    val s3Config = S3Config(bucketName = bucket.name)
    val dynamoConfig = DynamoConfig(table = table.name, Some(table.index))

    val vhsConfig = VHSConfig(
      dynamoConfig = dynamoConfig,
      s3Config = s3Config,
      globalS3Prefix = globalS3Prefix
    )

    val store = new VersionedHybridStore[T, Metadata, ObjectStore[T]](
      vhsConfig = vhsConfig,
      objectStore = objectStore,
      dynamoDbClient = dynamoDbClient
    )
    testWith(store)
  }

  @deprecated(
    "Call getJsonFor without passing the record parameter",
    "storage 2.0")
  def getJsonFor[T](bucket: Bucket, table: Table, record: T, id: String): String =
    getJsonFor(bucket = bucket, table = table, id = id)

  def getJsonFor(bucket: Bucket, table: Table, id: String): String = {
    val hybridRecord = getHybridRecord(table, id)

    getJsonFromS3(bucket, hybridRecord.s3key).noSpaces
  }

  def getContentFor(bucket: Bucket, table: Table, id: String) = {
    val hybridRecord = getHybridRecord(table, id)

    getContentFromS3(bucket, hybridRecord.s3key)
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
}
