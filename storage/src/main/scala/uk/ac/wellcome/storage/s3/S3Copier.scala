package uk.ac.wellcome.storage.s3

import java.io.InputStream

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.transfer.TransferManagerBuilder
import grizzled.slf4j.Logging
import org.apache.commons.io.IOUtils
import uk.ac.wellcome.storage.{ObjectCopier, ObjectLocation}

import scala.util.{Failure, Success}

class S3Copier(s3Client: AmazonS3) extends Logging with ObjectCopier {
  private val transferManager = TransferManagerBuilder.standard
    .withS3Client(s3Client)
    .build

  private val backend = new S3StorageBackend(s3Client)

  def copy(src: ObjectLocation, dst: ObjectLocation): Unit = {
    debug(s"Copying ${S3Urls.encode(src)} -> ${S3Urls.encode(dst)}")

    backend.get(dst) match {
      // If the destination object exists and is the same as the source
      // object, we can skip the copy operation.
      case Success(dstStream) =>
        backend.get(src) match {
          case Success(srcStream) =>
            compare(srcStream, dstStream)

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

          case Failure(err) => {
            dstStream.close()
            throw err
          }
        }

      case Failure(_) => transferFile(src, dst)
    }

    def compare(srcStream: InputStream, dstStream: InputStream): Unit = {
      if (IOUtils.contentEquals(srcStream, dstStream)) {
        debug(s"No-op copy: ${S3Urls.encode(src)} == ${S3Urls.encode(dst)}")
      } else {
        throw new RuntimeException(
          s"Destination object $dst exists and is different from $src!"
        )
      }
    }
  }

  private def transferFile(src: ObjectLocation, dst: ObjectLocation): Unit = {
    val copyTransfer = transferManager.copy(
      src.namespace,
      src.key,
      dst.namespace,
      dst.key
    )

    copyTransfer.waitForCopyResult()
  }
}
