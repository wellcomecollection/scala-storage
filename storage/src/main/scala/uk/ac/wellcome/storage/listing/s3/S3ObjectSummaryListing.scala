package uk.ac.wellcome.storage.listing.s3

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.iterable.S3Objects
import com.amazonaws.services.s3.model.S3ObjectSummary
import uk.ac.wellcome.storage.listing.Listing
import uk.ac.wellcome.storage.{ListingFailure, ObjectLocationPrefix}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class S3ObjectSummaryListing(batchSize: Int = 1000)(
  implicit s3Client: AmazonS3
) extends Listing[ObjectLocationPrefix, S3ObjectSummary] {
  override def list(prefix: ObjectLocationPrefix): ListingResult =
    Try {
      S3Objects
        .withPrefix(
          s3Client,
          prefix.namespace,
          prefix.path
        )
        .withBatchSize(batchSize)
        .asScala
    } match {
      case Failure(err)     => Left(ListingFailure(prefix, err))
      case Success(objects) => Right(objects)
    }
}
