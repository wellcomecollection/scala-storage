package uk.ac.wellcome.storage.transfer.s3

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.storage.listing.s3.S3ObjectLocationListing
import uk.ac.wellcome.storage.s3.{S3ObjectLocation, S3ObjectLocationPrefix}
import uk.ac.wellcome.storage.transfer.PrefixTransfer

class S3PrefixTransfer()(
  implicit
  val transfer: S3Transfer,
  val listing: S3ObjectLocationListing
) extends PrefixTransfer[S3ObjectLocationPrefix, S3ObjectLocation] {
  override protected def buildDstLocation(
    srcPrefix: S3ObjectLocationPrefix,
    dstPrefix: S3ObjectLocationPrefix,
    srcLocation: S3ObjectLocation): S3ObjectLocation =
    dstPrefix.asLocation(
      srcLocation.key.stripPrefix(srcPrefix.keyPrefix)
    )
}

object S3PrefixTransfer {
  def apply()(implicit s3Client: AmazonS3): S3PrefixTransfer = {
    implicit val transfer: S3Transfer = new S3Transfer()
    implicit val listing: S3ObjectLocationListing = S3ObjectLocationListing()

    new S3PrefixTransfer()
  }
}
