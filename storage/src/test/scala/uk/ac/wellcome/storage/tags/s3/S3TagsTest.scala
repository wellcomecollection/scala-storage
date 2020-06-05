package uk.ac.wellcome.storage.tags.s3

import com.amazonaws.services.s3.model._
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.tags.{Tags, TagsTestCases}

import scala.collection.JavaConverters._

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
}
