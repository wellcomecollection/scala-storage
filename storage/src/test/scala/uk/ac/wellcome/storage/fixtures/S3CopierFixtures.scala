package uk.ac.wellcome.storage.fixtures

import com.amazonaws.services.s3.model.PutObjectResult
import org.scalatest.Assertion
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3.Bucket

import scala.util.Random

trait S3CopierFixtures extends S3 {
  def createObjectLocationWith(
    bucket: Bucket = Bucket(randomAlphanumeric),
    key: String = randomAlphanumeric
  ): ObjectLocation =
    ObjectLocation(
      namespace = bucket.name,
      key = key
    )

  def createObjectLocation: ObjectLocation = createObjectLocationWith()

  def createObject(location: ObjectLocation): PutObjectResult =
    s3Client.putObject(
      location.namespace,
      location.key,
      randomAlphanumeric
    )

  def assertEqualObjects(x: ObjectLocation, y: ObjectLocation): Assertion =
    getContentFromS3(x) shouldBe getContentFromS3(y)

  private def randomAlphanumeric: String =
    Random.alphanumeric take 8 mkString
}
