package uk.ac.wellcome.storage.listing.s3

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.iterable.S3Objects
import com.amazonaws.services.s3.model.S3ObjectSummary
import uk.ac.wellcome.storage.{ListingFailure, ObjectLocationPrefix}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class S3ObjectSummaryListing(batchSize: Int = 1000)(
  implicit s3Client: AmazonS3
) extends S3Listing[S3ObjectSummary] {
  override def list(prefix: ObjectLocationPrefix): ListingResult =
    Try {
      val iterator = S3Objects
        .withPrefix(
          s3Client,
          prefix.namespace,
          prefix.path
        )
        .withBatchSize(batchSize)
        .asScala

      // Because the iterator is lazy, it won't make the initial call to S3 until
      // the caller starts to consume the results.  This can cause an exception to
      // be thrown in user code if, for example, the bucket doesn't exist.
      //
      // Although we discard the result of this toString method immediately, it
      // causes an exception to be thrown here and a Left returned, rather than
      // bubbling up the exception in user code.
      //
      // See the test cases in S3ListingTestCases.
      iterator.toString()

      iterator
    } match {
      case Failure(err)     => Left(ListingFailure(prefix, err))
      case Success(objects) => Right(objects)
    }
}
