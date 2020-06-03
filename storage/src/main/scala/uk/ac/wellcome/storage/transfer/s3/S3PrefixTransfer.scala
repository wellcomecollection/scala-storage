package uk.ac.wellcome.storage.transfer.s3

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.storage.listing.s3.S3ObjectLocationListing
import uk.ac.wellcome.storage.transfer.ObjectLocationPrefixTransfer

class S3PrefixTransfer()(
  implicit val transfer: S3Transfer,
  val listing: S3ObjectLocationListing
) extends ObjectLocationPrefixTransfer

object S3PrefixTransfer {
  def apply()(implicit s3Client: AmazonS3): S3PrefixTransfer = {
    implicit val transfer: S3Transfer = new S3Transfer
    implicit val listing: S3ObjectLocationListing = S3ObjectLocationListing()

    new S3PrefixTransfer()
  }
}
