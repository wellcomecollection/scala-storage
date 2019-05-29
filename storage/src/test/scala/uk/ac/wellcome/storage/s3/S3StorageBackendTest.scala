package uk.ac.wellcome.storage.s3

import com.amazonaws.services.s3.model.AmazonS3Exception
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.storage.fixtures.S3
import uk.ac.wellcome.storage.streaming.CodecInstances._

import scala.collection.JavaConverters._

class S3StorageBackendTest extends FunSpec with Matchers with S3 {
  val backend = new S3StorageBackend(s3Client)

  it("stores an object in S3") {
    withLocalS3Bucket { bucket =>
      val location = createObjectLocationWith(bucket)
      val content = "hello world"

      backend.put(
        location,
        inputStream = stringCodec.toStream(content).right.value,
        metadata = Map.empty) shouldBe Right(())

      stringCodec.fromStream(
        s3Client
          .getObject(location.namespace, location.key)
          .getObjectContent
      ).right.value shouldBe content
    }
  }

  it("can retrieve an object from S3") {
    withLocalS3Bucket { bucket =>
      val location = createObjectLocationWith(bucket)
      val content = "behind you!"

      s3Client.putObject(location.namespace, location.key, content)

      val result = backend.get(location)
      stringCodec.fromStream(result.right.value).right.value shouldBe content
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

      backend.put(
        location,
        inputStream = stringCodec.toStream(content).right.value,
        metadata = metadata) shouldBe Right(())

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
    val result = backend.get(createObjectLocationWith(namespace = createBucketName))

    val err = result.left.value.e
    err shouldBe a[AmazonS3Exception]
    err.getMessage should startWith("The specified bucket does not exist")
  }

  it("fails if asked to fetch a non-existent object") {
    withLocalS3Bucket { bucket =>
      val result = backend.get(createObjectLocationWith(bucket))

      val err = result.left.value.e
      err shouldBe a[AmazonS3Exception]
      err.getMessage should startWith("The specified key does not exist")
    }
  }
}
