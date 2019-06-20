package uk.ac.wellcome.storage.listing.s3

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.PutObjectResult
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.generators.ObjectLocationGenerators
import uk.ac.wellcome.storage.listing.Listing
import uk.ac.wellcome.storage.listing.fixtures.ListingFixtures

trait S3ListingFixtures[ListingResult] extends ObjectLocationGenerators with S3Fixtures with ListingFixtures[ObjectLocation, ObjectLocationPrefix, ListingResult, S3Listing[ListingResult], Bucket] {
  def createIdent(implicit bucket: Bucket): ObjectLocation = createObjectLocationWith(namespace = bucket.name)

  def extendIdent(location: ObjectLocation, extension: String): ObjectLocation =
    location.join(extension)

  def createPrefix: ObjectLocationPrefix = createObjectLocationPrefixWith(namespace = createBucketName)

  def createPrefixMatching(location: ObjectLocation): ObjectLocationPrefix = location.asPrefix

  def withListingContext[R](testWith: TestWith[Bucket, R]): R =
    withLocalS3Bucket { bucket =>
      testWith(bucket)
    }

  def createS3Listing(batchSize: Int = 1000)(implicit s3Client: AmazonS3 = s3Client): Listing[ObjectLocationPrefix, ListingResult]

  def createInitialEntries(bucket: Bucket, initialEntries: Seq[ObjectLocation]): Seq[PutObjectResult] =
    initialEntries
      .map { loc => s3Client.putObject(loc.namespace, loc.path, "hello world") }
}
