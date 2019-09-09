package uk.ac.wellcome.storage.store.s3

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.AmazonS3Exception
import org.scalatest.mockito.MockitoSugar
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.storage.{DoesNotExistError, StoreReadError}
import uk.ac.wellcome.storage.fixtures.S3Fixtures

class S3StreamReadableTest extends FunSpec with Matchers with S3Fixtures with EitherValues with MockitoSugar {
  def createS3ReadableWith(client: AmazonS3, retries: Int = 1): S3StreamReadable =
    new S3StreamReadable {
      override implicit val s3Client: AmazonS3 = client

      override val maxRetries: Int = retries
    }

  it("does not retry a deterministic error") {
    val spyClient = spy(s3Client)

    val readable = createS3ReadableWith(spyClient)

    withLocalS3Bucket { bucket =>
      val location = createObjectLocationWith(bucket)

      readable.get(location).left.value shouldBe a[DoesNotExistError]

      verify(spyClient, times(1)).getObject(location.namespace, location.path)
    }
  }

  it("retries a flaky error from S3") {
    val mockClient = mock[AmazonS3]

    withLocalS3Bucket { bucket =>
      val location = createObjectLocationWith(bucket)
      s3Client.putObject(
        location.namespace,
        location.path,
        "hello world"
      )

      when(mockClient.getObject(any[String], any[String]))
        .thenThrow(new AmazonS3Exception("We encountered an internal error. Please try again."))
        .thenReturn(s3Client.getObject(location.namespace, location.path))

      val readable = createS3ReadableWith(mockClient, retries = 3)
      readable.get(location) shouldBe a[Right[_, _]]

      verify(mockClient, times(2)).getObject(location.namespace, location.path)
    }
  }

  it("gives up if there are too many flaky errors") {
    val mockClient = mock[AmazonS3]

    val location = createObjectLocation

    val retries = 4

    when(mockClient.getObject(any[String], any[String]))
      .thenThrow(new AmazonS3Exception("We encountered an internal error. Please try again."))
      .thenThrow(new AmazonS3Exception("We encountered an internal error. Please try again."))
      .thenThrow(new AmazonS3Exception("We encountered an internal error. Please try again."))
      .thenThrow(new AmazonS3Exception("We encountered an internal error. Please try again."))

    val readable = createS3ReadableWith(mockClient, retries = retries)
    readable.get(location).left.value shouldBe a[StoreReadError]

    verify(mockClient, times(retries)).getObject(location.namespace, location.path)
  }
}
