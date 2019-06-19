package uk.ac.wellcome.storage.transfer.s3

import java.io.InputStream

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.transfer.TransferManagerBuilder
import org.apache.commons.io.IOUtils
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.store.s3.S3StreamStore
import uk.ac.wellcome.storage.transfer._

import scala.util.{Failure, Success, Try}

class S3Transfer(implicit s3Client: AmazonS3) extends Transfer[ObjectLocation] {
  private val transferManager = TransferManagerBuilder.standard
    .withS3Client(s3Client)
    .build

  private val streamStore = new S3StreamStore()

  override def transfer(src: ObjectLocation, dst: ObjectLocation): Either[TransferFailure, TransferSuccess[ObjectLocation]] = {
    def compare(srcStream: InputStream,
                dstStream: InputStream): Either[TransferOverwriteFailure[ObjectLocation], TransferNoOp[ObjectLocation]] = {
      if (IOUtils.contentEquals(srcStream, dstStream)) {
        Right(TransferNoOp(src, dst))
      } else {
        Left(TransferOverwriteFailure(src, dst))
      }
    }

    streamStore.get(dst) match {
      // If the destination object exists and is the same as the source
      // object, we can skip the copy operation.
      case Right(dstStream) =>
        streamStore.get(src) match {
          case Right(srcStream) =>
            val result = compare(srcStream.identifiedT, dstStream.identifiedT)

            // Remember to close the streams afterwards, or we might get
            // errors like
            //
            //    Unable to execute HTTP request: Timeout waiting for
            //    connection from pool
            //
            // See: https://github.com/wellcometrust/platform/issues/3600
            //      https://github.com/aws/aws-sdk-java/issues/269
            //
            srcStream.identifiedT.close()
            dstStream.identifiedT.close()

            result

          case Left(err) =>
            dstStream.identifiedT.close()
            Left(TransferSourceFailure(src, dst, err.e))
        }

      case _ => runTransfer(src, dst)
    }
  }

  private def runTransfer(src: ObjectLocation, dst: ObjectLocation): Either[TransferFailure, TransferSuccess[ObjectLocation]] = {
    val transfer = transferManager.copy(
      src.namespace,
      src.key,
      dst.namespace,
      dst.key
    )

    Try { transfer.waitForCopyResult() } match {
      case Success(_) => Right(TransferPerformed(src, dst))
      case Failure(err) => Left(TransferDestinationFailure(src, dst, err))
    }
  }
}
