package uk.ac.wellcome.storage.transfer.s3

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.transfer.TransferManagerBuilder
import uk.ac.wellcome.storage.ObjectLocation
import uk.ac.wellcome.storage.transfer._

import scala.util.{Failure, Success, Try}

class S3Transfer(implicit val s3Client: AmazonS3) extends Transfer[ObjectLocation] {
  private val transferManager = TransferManagerBuilder.standard
    .withS3Client(s3Client)
    .build

  override def transfer(src: ObjectLocation, dst: ObjectLocation): Either[TransferFailure, TransferSuccess] = {
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
