package uk.ac.wellcome.storage.transfer.s3

import java.io.InputStream

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{
  CopyObjectRequest,
  S3ObjectInputStream,
  StorageClass
}
import com.amazonaws.services.s3.transfer.TransferManagerBuilder
import org.apache.commons.io.IOUtils
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.transfer._

import scala.util.{Failure, Success, Try}

class S3Transfer(
  storageClass: StorageClass = StorageClass.StandardInfrequentAccess
)(implicit s3Client: AmazonS3)
    extends Transfer[ObjectLocation] {

  import uk.ac.wellcome.storage.RetryOps._

  private val transferManager = TransferManagerBuilder.standard
    .withS3Client(s3Client)
    .build

  override def transfer(
    src: ObjectLocation,
    dst: ObjectLocation): Either[TransferFailure, TransferSuccess] =
    getStream(dst) match {

      // If the destination object doesn't exist, we can go ahead and
      // start the transfer.
      //
      // We have seen once case where the S3 CopyObject API returned
      // a 500 error, in a bag with multiple 20GB+ files, so we do need
      // to be able to retry failures here.
      case Failure(_) =>
        def singleTransfer: Either[TransferFailure, TransferSuccess] =
          runTransfer(src, dst)

        singleTransfer.retry(maxAttempts = 3)

      case Success(dstStream) =>
        getStream(src) match {
          // If both the source and the destination exist, we can skip
          // the copy operation.
          case Success(srcStream) =>
            val result = compare(
              src = src,
              dst = dst,
              srcStream = srcStream,
              dstStream = dstStream
            )

            // Remember to close the streams afterwards, or we might get
            // errors like
            //
            //    Unable to execute HTTP request: Timeout waiting for
            //    connection from pool
            //
            // See: https://github.com/wellcometrust/platform/issues/3600
            //      https://github.com/aws/aws-sdk-java/issues/269
            //
            srcStream.abort()
            srcStream.close()
            dstStream.abort()
            dstStream.close()

            result

          case Failure(err) =>
            // As above, we need to abort the input stream so we don't leave streams
            // open or get warnings from the SDK.
            dstStream.abort()
            dstStream.close()
            Left(TransferSourceFailure(src, dst, err))
        }
    }

  private def compare(
    src: ObjectLocation,
    dst: ObjectLocation,
    srcStream: InputStream,
    dstStream: InputStream): Either[TransferOverwriteFailure[ObjectLocation],
                                    TransferNoOp[ObjectLocation]] =
    if (IOUtils.contentEquals(srcStream, dstStream)) {
      Right(TransferNoOp(src, dst))
    } else {
      Left(TransferOverwriteFailure(src, dst))
    }

  private def getStream(location: ObjectLocation): Try[S3ObjectInputStream] =
    Try {
      s3Client.getObject(location.namespace, location.path)
    }.map { _.getObjectContent }

  private def runTransfer(
    src: ObjectLocation,
    dst: ObjectLocation): Either[TransferFailure, TransferSuccess] = {
    val copyRequest =
      new CopyObjectRequest(src.namespace, src.path, dst.namespace, dst.path)
        .withStorageClass(storageClass)

    for {
      transfer <- Try {
        // This code will throw if the source object doesn't exist.
        transferManager.copy(copyRequest)
      } match {
        case Success(request) => Right(request)
        case Failure(err)     => Left(TransferSourceFailure(src, dst, err))
      }

      result <- Try {
        transfer.waitForCopyResult()
      } match {
        case Success(_)   => Right(TransferPerformed(src, dst))
        case Failure(err) => Left(TransferDestinationFailure(src, dst, err))
      }
    } yield result
  }
}
