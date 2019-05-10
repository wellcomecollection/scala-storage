package uk.ac.wellcome.storage.s3

import com.amazonaws.services.s3.model.AmazonS3Exception
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.storage.fixtures.{S3, StreamHelpers}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success}

class S3StorageBackendTest extends FunSpec with Matchers with S3 with StreamHelpers {
  val backend = new S3StorageBackend(s3Client)

  it("stores an object in S3") {
    withLocalS3Bucket { bucket =>
      val location = createObjectLocationWith(bucket)
      val content = "hello world"

      backend.put(location, input = toStream(content), metadata = Map.empty) shouldBe Success(())

      fromStream(
        s3Client
          .getObject(location.namespace, location.key)
          .getObjectContent
      ) shouldBe content
    }
  }

  it("can retrieve an object from S3") {
    withLocalS3Bucket { bucket =>
      val location = createObjectLocationWith(bucket)
      val content = "behind you!"

      s3Client.putObject(location.namespace, location.key, content)

      val result = backend.get(location)
      result shouldBe a[Success[_]]
      fromStream(result.get) shouldBe content
    }
  }

  it("stores metadata with the S3 object") {
    withLocalS3Bucket { bucket =>
      val location = createObjectLocationWith(bucket)
      val content = "你好"
      val metadata = Map(
        "language" -> "Chinese",
        "length" -> "2",
        "translation" -> "Hello!"
      )

      backend.put(location, input = toStream(content), metadata = metadata) shouldBe Success(())

      val storedMetadata =
        s3Client
          .getObject(location.namespace, location.key)
          .getObjectMetadata
          .getUserMetadata
          .asScala

      storedMetadata shouldBe metadata
    }
  }

  it("fails if asked to fetch from a non-existent bucket") {
    val result = backend.get(createObjectLocation)

    result shouldBe a[Failure[_]]
    result.failed.get shouldBe a[AmazonS3Exception]
    result.failed.get.getMessage should startWith("The specified bucket does not exist")
  }

  it("fails if asked to fetch a non-existent object") {
    withLocalS3Bucket { bucket =>
      val result = backend.get(createObjectLocationWith(bucket))

      result shouldBe a[Failure[_]]
      result.failed.get shouldBe a[AmazonS3Exception]
      result.failed.get.getMessage should startWith("The specified key does not exist")
    }
  }
}
