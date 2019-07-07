package uk.ac.wellcome.storage

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.storage.fixtures.S3Fixtures

class S3ObjectLocationTest extends FunSpec with Matchers with S3Fixtures {
  val bucket: String = createBucketName

  it("can join with a single path") {
    val root = S3ObjectLocation(bucket, key = "images/")
    val file = S3ObjectLocation(bucket, key = "images/001.jpg")

    root.join("001.jpg") shouldBe file
  }

  it("adds the trailing slash") {
    val root = S3ObjectLocation(bucket, key = "images")
    val file = S3ObjectLocation(bucket, key = "images/001.jpg")

    root.join("001.jpg") shouldBe file
  }

  it("can join multiple parts") {
    val root = S3ObjectLocation(bucket, key = "images")
    val file = S3ObjectLocation(bucket, key = "images/red/dogs/001.jpg")

    root.join("red", "dogs", "001.jpg") shouldBe file
  }
}
