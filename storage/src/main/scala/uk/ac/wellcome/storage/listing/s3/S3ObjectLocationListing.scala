package uk.ac.wellcome.storage.listing.s3

import com.amazonaws.services.s3.AmazonS3
import uk.ac.wellcome.storage.{S3ObjectLocation, S3ObjectLocationPrefix}

class S3ObjectLocationListing(implicit summaryListing: S3ObjectSummaryListing)
    extends S3Listing[S3ObjectLocation] {
  override def list(prefix: S3ObjectLocationPrefix): ListingResult =
    summaryListing
      .list(prefix)
      .map { iterator =>
        iterator.map { summary =>
          S3ObjectLocation(
            bucket = summary.getBucketName,
            key = summary.getKey
          )
        }
      }
}

object S3ObjectLocationListing {
  def apply(batchSize: Int = 1000)(
    implicit s3Client: AmazonS3): S3ObjectLocationListing = {
    implicit val summaryListing: S3ObjectSummaryListing =
      new S3ObjectSummaryListing(batchSize)

    new S3ObjectLocationListing()
  }
}
