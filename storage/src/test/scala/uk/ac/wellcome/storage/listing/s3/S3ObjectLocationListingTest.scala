package uk.ac.wellcome.storage.listing.s3

import com.amazonaws.services.s3.AmazonS3
import org.scalatest.Assertion
import uk.ac.wellcome.storage.s3.S3ObjectLocation

class S3ObjectLocationListingTest extends S3ListingTestCases[S3ObjectLocation] {
  override def assertResultCorrect(result: Iterable[S3ObjectLocation], entries: Seq[S3ObjectLocation]): Assertion =
    result.toSeq should contain theSameElementsAs entries

  override def createS3Listing(batchSize: Int = 1000)(implicit s3Client: AmazonS3 = s3Client): S3Listing[S3ObjectLocation] =
    S3ObjectLocationListing(batchSize)
}
