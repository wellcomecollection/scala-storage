package uk.ac.wellcome.storage.transfer.s3

import java.io.InputStream

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.S3ObjectInputStream
import com.amazonaws.services.s3.transfer.TransferManagerBuilder
import org.apache.commons.io.IOUtils
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.transfer._

import scala.util.{Failure, Success, Try}

class S3Transfer(implicit s3Client: AmazonS3) extends Transfer[ObjectLocation] {
  private val transferManager = TransferManagerBuilder.standard
    .withS3Client(s3Client)
    .build

  override def transfer(
    src: ObjectLocation,
    dst: ObjectLocation): Either[TransferFailure, TransferSuccess] = {
    def compare(
      srcStream: InputStream,
      dstStream: InputStream): Either[TransferOverwriteFailure[ObjectLocation],
                                      TransferNoOp[ObjectLocation]] = {
      if (IOUtils.contentEquals(srcStream, dstStream)) {
        Right(TransferNoOp(src, dst))
      } else {
        Left(TransferOverwriteFailure(src, dst))
      }
    }

    def getStream(location: ObjectLocation): Try[S3ObjectInputStream] =
      Try {
        s3Client.getObject(location.namespace, location.path)
      }.map { _.getObjectContent }

    val trySrcStream = getStream(src)
    val tryDstStream = getStream(dst)

    (trySrcStream, tryDstStream) match {
      // If the destination object exists and is the same as the source
      // object, we can skip the copy operation.
      case (Success(srcStream), Success(dstStream)) =>
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

      case (Success(srcStream), _) =>
        // We have to abort() rather than close() the connection, or we get
        // warnings like:
        //
        //      Not all bytes were read from the S3ObjectInputStream, aborting HTTP connection.
        //      This is likely an error and may result in sub-optimal behavior. Request only
        //      the bytes you need via a ranged GET or drain the input stream after use.
        //
        // In our case it isn't an error -- if there's no dst stream to compare to,
        // we don't need to read the original stream.
        srcStream.abort()

        runTransfer(src, dst)

      case (Failure(err), Success(dstStream)) =>
        // As above, we need to abort the input stream so we don't leave streams
        // open or get warnings from the SDK.
        dstStream.abort()
        Left(TransferSourceFailure(src, dst, err))

      case (Failure(err), _) =>
        Left(TransferSourceFailure(src, dst, err))
    }
  }

  private def runTransfer(
    src: ObjectLocation,
    dst: ObjectLocation): Either[TransferFailure, TransferSuccess] = {
    val transfer = transferManager.copy(
      src.namespace,
      src.path,
      dst.namespace,
      dst.path
    )

    Try { transfer.waitForCopyResult() } match {
      case Success(_)   => Right(TransferPerformed(src, dst))
      case Failure(err) => Left(TransferDestinationFailure(src, dst, err))
    }
  }
}
