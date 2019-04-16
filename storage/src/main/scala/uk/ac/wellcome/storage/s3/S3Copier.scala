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

    val trySrcInputStream = getInputStream(src)
    val tryDstInputStream = getInputStream(dst)

    // If the destination object exists and is the same as the
    // source object, we can skip doing the copy.
    (trySrcInputStream, tryDstInputStream) match {
      case (Success(srcStream), Success(dstStream)) =>
        compare(srcStream, dstStream)
      case (Success(_), _) => transferFile(src, dst)
      case (Failure(err), _) => throw err
    }

    def compare(srcStream: InputStream, dstStream: InputStream) {
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
