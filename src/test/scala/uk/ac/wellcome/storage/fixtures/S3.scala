package uk.ac.wellcome.storage.fixtures

import com.amazonaws.services.s3.AmazonS3
import grizzled.slf4j.Logging
import io.circe.{Decoder, Json}
import io.circe.parser.parse
import org.scalatest.Matchers
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.s3.{S3ClientFactory, S3Config, S3StorageBackend}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

object S3 {

  class Bucket(val name: String) extends AnyVal {
    override def toString = s"S3.Bucket($name)"
  }

  object Bucket {
    def apply(name: String): Bucket = new Bucket(name)
  }

}

trait S3 extends Logging with Eventually with IntegrationPatience with Matchers {

  import S3._

  protected val localS3EndpointUrl = "http://localhost:33333"
  private val regionName = "localhost"

  protected val accessKey = "accessKey1"
  protected val secretKey = "verySecretKey1"

  def s3LocalFlags(bucket: Bucket) = s3ClientLocalFlags ++ Map(
    "aws.s3.bucketName" -> bucket.name
  )

  def s3ClientLocalFlags = Map(
    "aws.s3.endpoint" -> localS3EndpointUrl,
    "aws.s3.accessKey" -> accessKey,
    "aws.s3.secretKey" -> secretKey,
    "aws.s3.region" -> regionName
  )

  val s3Client: AmazonS3 = S3ClientFactory.create(
    region = regionName,
    endpoint = localS3EndpointUrl,
    accessKey = accessKey,
    secretKey = secretKey
  )

  implicit val storageBackend = new S3StorageBackend(s3Client)

  def withLocalS3Bucket[R] =
    fixture[Bucket, R](
      create = {
        eventually {
          s3Client.listBuckets().asScala.size should be >= 0
        }
        val bucketName: String =
          (Random.alphanumeric take 10 mkString).toLowerCase
        s3Client.createBucket(bucketName)
        eventually { s3Client.doesBucketExistV2(bucketName) }

        Bucket(bucketName)
      },
      destroy = { bucket: Bucket =>
        listKeysInBucket(bucket).foreach { key =>
          safeCleanup(key) { s3Client.deleteObject(bucket.name, _) }
        }

        s3Client.deleteBucket(bucket.name)
      }
    )

  def getContentFromS3(bucket: Bucket, key: String): String =
    scala.io.Source
      .fromInputStream(
        s3Client.getObject(bucket.name, key).getObjectContent
      )
      .mkString

  def getContentFromS3(location: ObjectLocation): String =
    getContentFromS3(bucket = Bucket(location.namespace), key = location.key)

  def getJsonFromS3(bucket: Bucket, key: String): Json =
    parse(getContentFromS3(bucket, key)).right.get

  def getJsonFromS3(location: ObjectLocation): Json =
    getJsonFromS3(bucket = Bucket(location.namespace), key = location.key)

  def getObjectFromS3[T](bucket: Bucket, key: String)(
    implicit decoder: Decoder[T]): T =
    fromJson[T](getContentFromS3(bucket = bucket, key = key)).get

  def getObjectFromS3[T](location: ObjectLocation)(implicit decoder: Decoder[T]): T =
    getObjectFromS3[T](
      bucket = Bucket(location.namespace),
      key = location.key
    )

  /** Returns a list of keys in an S3 bucket.
    *
    * Note: this only makes a single call to the ListObjects API, so it
    * gets a single page of results.
    *
    * @param bucket The instance of [[S3.Bucket]] to list.
    * @return A list of object keys.
    */
  def listKeysInBucket(bucket: Bucket): List[String] =
    s3Client
      .listObjects(bucket.name)
      .getObjectSummaries
      .asScala
      .map { _.getKey }
      .toList

  /** Returns a map (key -> contents) for all objects in an S3 bucket.
    *
    * @param bucket The instance of [[S3.Bucket]] to read.
    *
    */
  def getAllObjectContents(bucket: Bucket): Map[String, String] =
    listKeysInBucket(bucket).map { key =>
      key -> getContentFromS3(bucket = bucket, key = key)
    }.toMap

  def createS3ConfigWith(bucket: Bucket): S3Config =
    S3Config(bucketName = bucket.name)
}
