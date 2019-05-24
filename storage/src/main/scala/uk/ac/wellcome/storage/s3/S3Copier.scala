package uk.ac.wellcome.storage.s3

import java.io.InputStream

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.transfer.TransferManagerBuilder
import grizzled.slf4j.Logging
import org.apache.commons.io.IOUtils
import uk.ac.wellcome.storage.{BackendWriteError, ObjectCopier, ObjectLocation, StorageError}

import scala.util.{Failure, Success, Try}

class S3Copier(s3Client: AmazonS3) extends Logging with ObjectCopier {
  private val transferManager = TransferManagerBuilder.standard
    .withS3Client(s3Client)
    .build

  private val backend = new S3StorageBackend(s3Client)

  def copy(src: ObjectLocation, dst: ObjectLocation): Either[StorageError, Unit] = {
    debug(s"Copying ${S3Urls.encode(src)} -> ${S3Urls.encode(dst)}")

    def compare(srcStream: InputStream, dstStream: InputStream): Either[StorageError, Unit] = {
      if (IOUtils.contentEquals(srcStream, dstStream)) {
        debug(s"No-op copy: ${S3Urls.encode(src)} == ${S3Urls.encode(dst)}")
        Right(())
      } else {
        Left(
          BackendWriteError(
            new Throwable(s"Destination object $dst exists and is different from $src!")
          )
        )
      }
    }

    backend.get(dst) match {
      // If the destination object exists and is the same as the source
      // object, we can skip the copy operation.
      case Right(dstStream) =>
        backend.get(src) match {
          case Right(srcStream) =>
            val result = compare(srcStream, dstStream)

            // Remember to close the streams afterwards, or we might get
            // errors like
            //
            //    Unable to execute HTTP request: Timeout waiting for
            //    connection from pool
            //
            // See: https://github.com/wellcometrust/platform/issues/3600
            //      https://github.com/aws/aws-sdk-java/issues/269
            //
            srcStream.close()
            dstStream.close()
            result

          case Left(err) =>
            dstStream.close()
            Left(err)
        }

      case _ => transferFile(src, dst)
    }
  }

  private def transferFile(src: ObjectLocation, dst: ObjectLocation): Either[BackendWriteError, Unit] = {
    val copyTransfer = transferManager.copy(
      src.namespace,
      src.key,
      dst.namespace,
      dst.key
    )

    Try {
      copyTransfer.waitForCopyResult()
    } match {
      case Success(_) => Right(())
      case Failure(err) => Left(BackendWriteError(err))
    }
  }
}
