package uk.ac.wellcome.storage.s3

import java.io.InputStream

import com.amazonaws.services.s3.AmazonS3
import grizzled.slf4j.Logging
import org.apache.commons.io.IOUtils
import uk.ac.wellcome.storage.{ObjectCopier, ObjectLocation}

import scala.util.{Failure, Success, Try}

class S3Copier(s3Client: AmazonS3) extends Logging with ObjectCopier {
  def copy(src: ObjectLocation, dst: ObjectLocation): Unit = {
    debug(s"Copying ${s3Uri(src)} -> ${s3Uri(dst)}")

    val trySrcInputStream = getInputStream(src)
    val tryDstInputStream = getInputStream(dst)

    // If the destination object exists and is the same as the
    // source object, we can skip doing the copy.
    (trySrcInputStream, tryDstInputStream) match {
      case (Success(srcInputStream), Success(dstInputStream)) =>
        if (IOUtils.contentEquals(srcInputStream, dstInputStream)) {
          debug(s"No-op copy: ${s3Uri(src)} == ${s3Uri(dst)}")
          ()
        } else {
          throw new RuntimeException(
            s"Destination object $dst exists and is different from $src!"
          )
        }
      case (Success(_), _) =>
        s3Client.copyObject(
          src.namespace,
          src.key,
          dst.namespace,
          dst.key
        )
      case (Failure(err), _) => throw err
    }
  }

  private def getInputStream(objectLocation: ObjectLocation): Try[InputStream] = Try {
    s3Client
      .getObject(objectLocation.namespace, objectLocation.key)
      .getObjectContent
  }

  private def s3Uri(objectLocation: ObjectLocation): String =
    s"s3://${objectLocation.namespace}/${objectLocation.key}"
}
