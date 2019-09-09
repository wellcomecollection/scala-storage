package uk.ac.wellcome.storage.store.s3

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{AmazonS3Exception, S3Object}
import grizzled.slf4j.Logging
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.store.Readable
import uk.ac.wellcome.storage.streaming._

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

trait S3StreamReadable
    extends Readable[ObjectLocation, InputStreamWithLengthAndMetadata]
    with Logging {
  implicit val s3Client: AmazonS3
  val maxRetries: Int

  import RetryOps._

  override def get(location: ObjectLocation): ReadEither = {
    val retryableGet = (getOnce _).retry(maxRetries)

    retryableGet(location)
  }

  private def getOnce(location: ObjectLocation): ReadEither = {
    Try(s3Client.getObject(location.namespace, location.path)) match {
      case Success(retrievedObject: S3Object) =>
        Right(
          Identified(location, buildStoreEntry(retrievedObject))
        )
      case Failure(err) => Left(buildGetError(err))
    }
  }

  private def buildGetError(throwable: Throwable): ReadError =
    throwable match {
      case exc: AmazonS3Exception
          if exc.getMessage.startsWith("The specified key does not exist") ||
            exc.getMessage.startsWith("The specified bucket does not exist") =>
        DoesNotExistError(exc)

      case exc: AmazonS3Exception
          if exc.getMessage.startsWith("The specified bucket is not valid") =>
        StoreReadError(exc)

      case exc: AmazonS3Exception
          if exc.getMessage.startsWith("Read timed out") ||
            exc.getMessage.startsWith(
              "We encountered an internal error. Please try again.") =>
        new StoreReadError(exc) with RetryableError

      case _ => StoreReadError(throwable)
    }

  private def buildStoreEntry(
    s3Object: S3Object): InputStreamWithLengthAndMetadata = {
    // We get a mutable.Map from the S3 SDK, but we want an immutable Map to pass
    // out to the codebase, hence this slightly odd construction!
    val userMetadata = s3Object.getObjectMetadata.getUserMetadata
    val metadata = userMetadata
      .keySet()
      .asScala
      .map { k =>
        (k, userMetadata.get(k))
      }
      .toMap

    new InputStreamWithLengthAndMetadata(
      s3Object.getObjectContent,
      length = s3Object.getObjectMetadata.getContentLength,
      metadata = metadata
    )
  }
}
