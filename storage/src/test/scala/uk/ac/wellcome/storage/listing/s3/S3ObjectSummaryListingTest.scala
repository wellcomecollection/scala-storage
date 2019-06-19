package uk.ac.wellcome.storage.listing.s3

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.S3ObjectSummary
import org.scalatest.Assertion
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}
import uk.ac.wellcome.storage.listing.Listing

class S3ObjectSummaryListingTest extends S3ListingTestCases[S3ObjectSummary] {
  override def assertResultCorrect(result: Iterable[S3ObjectSummary], entries: Seq[ObjectLocation]): Assertion = {
    val actualLocations =
      result
        .toSeq
        .map { summary => ObjectLocation(summary.getBucketName, summary.getKey) }

    actualLocations should contain theSameElementsAs entries
  }

  override def createS3Listing(batchSize: Int = 1000)(implicit s3Client: AmazonS3 = s3Client): Listing[ObjectLocationPrefix, S3ObjectSummary] = new S3ObjectSummaryListing(batchSize)
}
