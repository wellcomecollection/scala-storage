package uk.ac.wellcome.storage.fixtures

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.iterable.S3Objects
import com.amazonaws.services.s3.model.S3ObjectSummary
import grizzled.slf4j.Logging
import io.circe.{Decoder, Json}
import io.circe.parser.parse
import org.scalatest.{Assertion, EitherValues, Matchers}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import uk.ac.wellcome.fixtures._
import uk.ac.wellcome.json.JsonUtil._
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.generators.ObjectLocationGenerators
import uk.ac.wellcome.storage.s3.{S3ClientFactory, S3Config, S3StorageBackend}
import uk.ac.wellcome.storage.streaming.CodecInstances._

import scala.collection.JavaConverters._

object S3 {
  class Bucket(val name: String) extends AnyVal {
    override def toString = s"S3.Bucket($name)"
  }

  object Bucket {
    def apply(name: String): Bucket = new Bucket(name)
  }
}

trait S3 extends Logging with Eventually with IntegrationPatience with Matchers with EitherValues with ObjectLocationGenerators {

  import S3._

  protected val localS3EndpointUrl = "http://localhost:33333"
  private val regionName = "localhost"

  protected val accessKey = "accessKey1"
  protected val secretKey = "verySecretKey1"

  implicit val s3Client: AmazonS3 = S3ClientFactory.create(
    region = regionName,
    endpoint = localS3EndpointUrl,
    accessKey = accessKey,
    secretKey = secretKey
  )

  implicit val s3StorageBackend: S3StorageBackend =
    new S3StorageBackend(s3Client)

  def withLocalS3Bucket[R]: Fixture[Bucket, R] =
    fixture[Bucket, R](
      create = {
        eventually {
          s3Client.listBuckets().asScala.size should be >= 0
        }
        val bucketName: String = createBucketName
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

  def getContentFromS3(location: ObjectLocation): String =
    s3StorageBackend.get(location).flatMap { stringCodec.fromStream }.right.value

  def getJsonFromS3(location: ObjectLocation): Json =
    parse(getContentFromS3(location)).right.get

  def getObjectFromS3[T](location: ObjectLocation)(implicit decoder: Decoder[T]): T =
    fromJson[T](getContentFromS3(location)).get

  def createBucketName: String =
    // Bucket names
    //  - start with a lowercase letter or number,
    //  - do not contain uppercase characters or underscores,
    //  - between 3 and 63 characters in length.
    // [https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html#bucketnamingrules]
    randomAlphanumeric.toLowerCase

  def createBucket: Bucket =
    Bucket(createBucketName)

  def createObjectLocationWith(
    bucket: Bucket
  ): ObjectLocation =
    ObjectLocation(
      namespace = bucket.name,
      key = randomAlphanumeric
    )

  def createObjectLocationWith(
    bucket: Bucket,
    key: String
  ): ObjectLocation =
    ObjectLocation(
      namespace = bucket.name,
      key = key
    )

  def createObject(location: ObjectLocation,
                   content: String = randomAlphanumeric): Unit =
    stringCodec
      .toStream(content)
      .flatMap { inputStream =>
        s3StorageBackend.put(location = location, inputStream = inputStream)
      }
      .right.value

  def assertEqualObjects(x: ObjectLocation, y: ObjectLocation): Assertion =
    getContentFromS3(x) shouldBe getContentFromS3(y)

  /** Returns a list of keys in an S3 bucket.
    *
    * @param bucket The instance of S3.Bucket to list.
    * @return A list of object keys.
    */
  def listKeysInBucket(bucket: Bucket): List[String] =
    S3Objects
      .inBucket(s3Client, bucket.name)
      .withBatchSize(1000)
      .iterator()
      .asScala
      .toList
      .par
      .map { objectSummary: S3ObjectSummary =>
        objectSummary.getKey
      }
      .toList

  /** Returns a map (key -> contents) for all objects in an S3 bucket.
    *
    * @param bucket The instance of S3.Bucket to read.
    *
    */
  def getAllObjectContents(bucket: Bucket): Map[String, String] =
    listKeysInBucket(bucket).map { key =>
      key -> getContentFromS3(createObjectLocationWith(bucket, key))
    }.toMap

  def createS3ConfigWith(bucket: Bucket): S3Config =
    S3Config(bucketName = bucket.name)
}
