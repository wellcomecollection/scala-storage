package uk.ac.wellcome.storage.s3

import java.net.SocketTimeoutException

import com.amazonaws.services.s3.model.AmazonS3Exception
import grizzled.slf4j.Logging
import uk.ac.wellcome.storage._

import scala.util.{Failure, Success, Try}

object S3Get extends Logging {
  import RetryOps._

  def get[T](location: ObjectLocation, maxRetries: Int)(
    getFunction: ObjectLocation => T
  ): Either[ReadError, T] = {
    val retryableGet = (getOnce(getFunction)(_)).retry(maxRetries)

    retryableGet(location)
  }

  private def getOnce[T](getFunction: ObjectLocation => T): ObjectLocation => Either[ReadError, T] =
    (location: ObjectLocation) =>
      Try { getFunction(location) } match {
        case Success(t)   => Right(t)
        case Failure(err) => Left(buildGetError(err))
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
