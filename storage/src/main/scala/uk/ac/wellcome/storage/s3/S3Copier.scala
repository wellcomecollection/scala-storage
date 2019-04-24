package uk.ac.wellcome.storage.s3

import java.io.InputStream

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.transfer.TransferManagerBuilder
import grizzled.slf4j.Logging
import org.apache.commons.io.IOUtils
import uk.ac.wellcome.storage.{ObjectCopier, ObjectLocation}

import scala.util.{Failure, Success, Try}

class S3Copier(s3Client: AmazonS3) extends Logging with ObjectCopier {
  private val transferManager = TransferManagerBuilder.standard
    .withS3Client(s3Client)
    .build

  def copy(src: ObjectLocation, dst: ObjectLocation): Unit = {
    debug(s"Copying ${s3Uri(src)} -> ${s3Uri(dst)}")

    getInputStream(dst) match {
      // If the destination object exists and is the same as the source
      // object, we can skip the copy operation.
      case Success(dstStream) =>
        getInputStream(src) match {
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
        debug(s"No-op copy: ${s3Uri(src)} == ${s3Uri(dst)}")
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

  private def getInputStream(objectLocation: ObjectLocation) = Try {
    s3Client
      .getObject(objectLocation.namespace, objectLocation.key)
      .getObjectContent
  }

  private def s3Uri(objectLocation: ObjectLocation): String =
    s"s3://${objectLocation.namespace}/${objectLocation.key}"
}
