package uk.ac.wellcome.storage.listing

import com.amazonaws.SdkClientException
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{AmazonS3Exception, PutObjectResult, S3ObjectSummary}
import org.scalatest.{Assertion, EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.generators.{ObjectLocationGenerators, RandomThings}
import uk.ac.wellcome.storage.listing.memory.MemoryListing
import uk.ac.wellcome.storage.listing.s3.S3ObjectSummaryListing
import uk.ac.wellcome.storage.store.memory.MemoryStore
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}

trait ListingFixtures[Ident, Prefix, ListingResult, ListingContext] extends {
  def withListingContext[R](testWith: TestWith[ListingContext, R]): R

  def withListing[R](context: ListingContext, initialEntries: Seq[Ident])(testWith: TestWith[Listing[Prefix, ListingResult], R]): R
}

trait ListingTestCases[Ident, Prefix, ListingResult, ListingContext] extends FunSpec with Matchers with EitherValues with ObjectLocationGenerators with ListingFixtures[Ident, Prefix, ListingResult, ListingContext] {
  def createIdent(implicit context: ListingContext): Ident
  def extendIdent(id: Ident, extension: String): Ident
  def createPrefix: Prefix
  def createPrefixMatching(id: Ident): Prefix

  def assertResultCorrect(result: Iterable[ListingResult], entries: Seq[Ident]): Assertion

  describe("behaves as a listing") {
    it("doesn't find anything in an empty store") {
      withListingContext { implicit context =>
        val ident = createIdent
        val prefix = createPrefixMatching(ident)

        withListing(context, initialEntries = Seq.empty) { listing =>
          listing.list(prefix).right.value shouldBe empty
        }
      }
    }

    it("finds a single entry where the prefix matches the ident") {
      withListingContext { implicit context =>
        val ident = createIdent
        val prefix = createPrefixMatching(ident)
        val entries = Seq(ident)

        withListing(context, initialEntries = entries) { listing =>
          assertResultCorrect(
            result = listing.list(prefix).right.value,
            entries = entries
          )
        }
      }
    }

    it("finds a single entry where the prefix is strictly shorter than the ident") {
      withListingContext { implicit context =>
        val ident = createIdent
        val prefix = createPrefixMatching(ident)
        val entries = Seq(extendIdent(ident, randomAlphanumeric))

        withListing(context, initialEntries = entries) { listing =>
          assertResultCorrect(
            result = listing.list(prefix).right.value,
            entries = entries
          )
        }
      }
    }

    it("finds multiple matching entries") {
      withListingContext { implicit context =>
        val ident = createIdent
        val entries = Seq("1.txt", "2.txt", "3.txt").map { filename => extendIdent(ident, filename) }
        val prefix = createPrefixMatching(ident)

        withListing(context, initialEntries = entries) { listing =>
          assertResultCorrect(
            result = listing.list(prefix).right.value,
            entries = entries
          )
        }
      }
    }

    it("ignores entries that don't match") {
      withListingContext { implicit context =>
        val ident = createIdent
        val entries = Seq("1.txt", "2.txt", "3.txt").map { filename => extendIdent(ident, filename) }
        val prefix = createPrefixMatching(ident)

        val extraEntries = Seq(createIdent, createIdent)

        withListing(context, initialEntries = entries ++ extraEntries) { listing =>
          assertResultCorrect(
            result = listing.list(prefix).right.value,
            entries = entries
          )
        }
      }
    }
  }
}

trait MemoryListingFixtures[T] extends ListingFixtures[String, String, String, MemoryStore[String, T]] {
  def createT: T

  override def withListingContext[R](testWith: TestWith[MemoryStore[String, T], R]): R =
    testWith(
      new MemoryStore[String, T](
        initialEntries = Map.empty
      )
    )

  override def withListing[R](context: MemoryStore[String, T], initialEntries: Seq[String])(testWith: TestWith[Listing[String, String], R]): R =
    testWith(
      new MemoryListing[String, String, T] {
        override var entries: Map[String, T] = initialEntries.map { id => (id, createT) }.toMap

        override protected def startsWith(id: String, prefix: String): Boolean = id.startsWith(prefix)
      }
    )
}

class MemoryListingTest extends ListingTestCases[String, String, String, MemoryStore[String, Array[Byte]]] with MemoryListingFixtures[Array[Byte]] with RandomThings {
  def createT: Array[Byte] = randomBytes()

  override def createIdent(implicit context: MemoryStore[String, Array[Byte]]): String = randomAlphanumeric

  override def extendIdent(id: String, extension: String): String = id + extension

  override def createPrefix: String = randomAlphanumeric

  override def createPrefixMatching(id: String): String = id

  override def assertResultCorrect(result: Iterable[String], entries: Seq[String]): Assertion =
    result.toSeq should contain theSameElementsAs entries
}

trait S3ListingFixtures[ListingResult] extends ObjectLocationGenerators with S3Fixtures {
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

trait S3ListingTestCases[ListingResult] extends ListingTestCases[ObjectLocation, ObjectLocationPrefix, ListingResult, Bucket] with S3ListingFixtures[ListingResult] {
  def withListing[R](bucket: Bucket, initialEntries: Seq[ObjectLocation])(testWith: TestWith[Listing[ObjectLocationPrefix, ListingResult], R]): R = {
    createInitialEntries(bucket, initialEntries)

    testWith(createS3Listing())
  }

  val listing: Listing[ObjectLocationPrefix, ListingResult] = createS3Listing()

  describe("behaves as an S3 listing") {
    it("throws an exception if asked to list from a non-existent bucket") {
      val prefix = createPrefix

      val err = listing.list(prefix).left.value
      err.e.getMessage should startWith("The specified bucket does not exist")
      err.e shouldBe a[AmazonS3Exception]
    }

    it("ignores entries with a matching key in a different bucket") {
      withLocalS3Bucket { bucket =>
        val location = createObjectLocationWith(bucket)
        s3Client.putObject(location.namespace, location.path, "hello world")

        // Now create the same keys but in a different bucket
        withLocalS3Bucket { queryBucket =>
          val queryLocation = location.copy(namespace = queryBucket.name)
          val prefix = queryLocation.asPrefix

          listing.list(prefix).right.value shouldBe empty
        }
      }
    }

    it("handles an error from S3") {
      val prefix = createPrefix

      val brokenListing = createS3Listing()(s3Client = brokenS3Client)

      val err = brokenListing.list(prefix).left.value
      err.e.getMessage should startWith("Unable to execute HTTP request")
      err.e shouldBe a[SdkClientException]
    }

    it("ignores objects in the same bucket with a different key") {
      withLocalS3Bucket { bucket =>
        val location = createObjectLocationWith(bucket)
        s3Client.putObject(location.namespace, location.path, "hello world")

        val prefix = createObjectLocationWith(bucket).asPrefix
        listing.list(prefix).right.value shouldBe empty
      }
    }

    it("fetches all the objects, not just the batch size") {
      withLocalS3Bucket { bucket =>
        val location = createObjectLocationWith(bucket)

        val locations = (1 to 10).map { i => location.join(s"file_$i.txt") }
        createInitialEntries(bucket, locations)

        val smallBatchListing = createS3Listing(batchSize = 5)
        assertResultCorrect(
          smallBatchListing.list(location.asPrefix).right.value,
          entries = locations
        )
      }
    }
  }
}

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