package uk.ac.wellcome.storage.store.s3

import java.net.SocketTimeoutException

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{AmazonS3Exception, S3Object}
import grizzled.slf4j.Logging
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.store.Readable
import uk.ac.wellcome.storage.streaming._

import scala.util.{Failure, Success, Try}

trait S3StreamReadable
    extends Readable[ObjectLocation, InputStreamWithLength]
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
          Identified(
            location,
            new InputStreamWithLength(
              retrievedObject.getObjectContent,
              length = retrievedObject.getObjectMetadata.getContentLength
            )
          )
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
          if exc.getMessage.startsWith(
            "We encountered an internal error. Please try again.") ||
            exc.getMessage.startsWith("Please reduce your request rate.") =>
        new StoreReadError(exc) with RetryableError

      case exc: SocketTimeoutException =>
        new StoreReadError(exc) with RetryableError

      case _ =>
        warn(s"Unrecognised error inside S3StreamStore.get: $throwable")
        StoreReadError(throwable)
    }
}
