package uk.ac.wellcome.storage.tags.s3

import com.amazonaws.services.s3.model._
import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.{ObjectLocation, UpdateError, UpdateWriteError}
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.tags.{Tags, TagsTestCases}

import scala.collection.JavaConverters._
import scala.util.Random

class S3TagsTest extends AnyFunSpec with Matchers with TagsTestCases[ObjectLocation, Bucket] with S3Fixtures {
  // We can associate with at most 10 tags on an object; see
  // https://docs.aws.amazon.com/AmazonS3/latest/dev/object-tagging.html
  override val maxTags: Int = 10

  override def withTags[R](initialTags: Map[ObjectLocation, Map[String, String]])(testWith: TestWith[Tags[ObjectLocation], R]): R = {
    initialTags
      .foreach { case (location, tags) =>
        s3Client.putObject(location.namespace, location.path, randomAlphanumeric)

        val tagSet = tags
          .map { case (k, v) => new Tag(k, v) }
          .toSeq
          .asJava

        s3Client.setObjectTagging(
          new SetObjectTaggingRequest(
            location.namespace,
            location.path,
            new ObjectTagging(tagSet)
          )
        )
      }

    testWith(new S3Tags())
  }

  override def createIdent(bucket: Bucket): ObjectLocation =
    createObjectLocationWith(bucket)

  override def withContext[R](testWith: TestWith[Bucket, R]): R =
    withLocalS3Bucket { bucket =>
      testWith(bucket)
    }

  val s3Tags = new S3Tags()

  describe("handles S3-specific errors") {
    // We can associate with at most 10 tags on an object; see
    // https://docs.aws.amazon.com/AmazonS3/latest/dev/object-tagging.html
    it("if you send more than 10 tags") {
      val newTags = (1 to 11)
        .map { i => s"key-$i" -> s"value-$i" }
        .toMap

      withLocalS3Bucket { bucket =>
        val location = createObjectLocationWith(bucket)
        s3Client.putObject(location.namespace, location.path, randomAlphanumeric)

        val result =
          s3Tags
            .update(location) { existingTags: Map[String, String] =>
              Right(existingTags ++ newTags)
            }

        assertIsS3Exception(result) {
          _ should startWith("Object tags cannot be greater than 10")
        }
      }
    }

    it("if the tag name is empty") {
      withLocalS3Bucket { bucket =>
        val location = createObjectLocationWith(bucket)
        s3Client.putObject(location.namespace, location.path, randomAlphanumeric)

        val result =
          s3Tags
            .update(location) { existingTags: Map[String, String] =>
              Right(existingTags ++ Map("" -> "value"))
            }

        assertIsS3Exception(result) {
          _ should startWith("The TagKey you have provided is invalid")
        }
      }
    }

    it("if the tag name is too long") {
      // A tag key can be at most 128 characters long.
      // https://docs.aws.amazon.com/AmazonS3/latest/dev/object-tagging.html
      withLocalS3Bucket { bucket =>
        val location = createObjectLocationWith(bucket)
        s3Client.putObject(location.namespace, location.path, randomAlphanumeric)

        val result =
          s3Tags
            .update(location) { existingTags: Map[String, String] =>
              Right(existingTags ++ Map(randomAlphanumericWithLength(129) -> "value"))
            }

        assertIsS3Exception(result) {
          _ should startWith("The TagKey you have provided is invalid")
        }
      }
    }

    it("if the tag value is too long") {
      // A tag value can be at most 256 characters long.
      // https://docs.aws.amazon.com/AmazonS3/latest/dev/object-tagging.html
      withLocalS3Bucket { bucket =>
        val location = createObjectLocationWith(bucket)
        s3Client.putObject(location.namespace, location.path, randomAlphanumeric)

        val result =
          s3Tags
            .update(location) { existingTags: Map[String, String] =>
              Right(existingTags ++ Map("key" -> randomAlphanumericWithLength(257)))
            }

        assertIsS3Exception(result) {
          _ should startWith("The TagValue you have provided is invalid")
        }
      }
    }
  }

  private def assertIsS3Exception(result: Either[UpdateError, Map[String, String]])(assert: String => Assertion): Assertion = {
    val err = result.left.value

    err shouldBe a[UpdateWriteError]
    err.e shouldBe a[AmazonS3Exception]
    assert(err.e.getMessage)
  }

  def randomAlphanumericWithLength(length: Int): String =
    Random.alphanumeric take length mkString
}
