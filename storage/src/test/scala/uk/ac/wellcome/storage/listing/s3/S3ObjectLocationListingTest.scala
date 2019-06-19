package uk.ac.wellcome.storage.listing.s3

import com.amazonaws.services.s3.AmazonS3
import org.scalatest.Assertion
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}
import uk.ac.wellcome.storage.listing.Listing

class S3ObjectLocationListingTest extends S3ListingTestCases[ObjectLocation] {
  override def assertResultCorrect(result: Iterable[ObjectLocation], entries: Seq[ObjectLocation]): Assertion =
    result.toSeq should contain theSameElementsAs entries

  override def createS3Listing(batchSize: Int = 1000)(implicit s3Client: AmazonS3 = s3Client): Listing[ObjectLocationPrefix, ObjectLocation] =
    S3ObjectLocationListing(batchSize)
}
