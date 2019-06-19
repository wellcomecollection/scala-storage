package uk.ac.wellcome.storage.listing

import com.amazonaws.services.s3.model.{PutObjectResult, S3ObjectSummary}
import org.scalatest.{Assertion, EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.fixtures.S3Fixtures
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.generators.{ObjectLocationGenerators, RandomThings}
import uk.ac.wellcome.storage.listing.memory.MemoryListing
import uk.ac.wellcome.storage.listing.s3.S3ObjectSummaryListing
import uk.ac.wellcome.storage.store.memory.MemoryStore

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

  describe("behaves as an object location listing") {
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

trait S3ListingFixtures extends ObjectLocationGenerators with S3Fixtures {
  def createIdent(implicit bucket: Bucket): ObjectLocation = createObjectLocationWith(namespace = bucket.name)

  def extendIdent(location: ObjectLocation, extension: String): ObjectLocation =
    location.join(extension)

  def createPrefix: ObjectLocationPrefix = createObjectLocationPrefix

  def createPrefixMatching(location: ObjectLocation): ObjectLocationPrefix = location.asPrefix

  def withListingContext[R](testWith: TestWith[Bucket, R]): R =
    withLocalS3Bucket { bucket =>
      testWith(bucket)
    }

  def createInitialEntries(bucket: Bucket, initialEntries: Seq[ObjectLocation]): Seq[PutObjectResult] =
    initialEntries
      .map { loc => s3Client.putObject(loc.namespace, loc.path, "hello world") }
}

class S3ObjectSummaryListingTest extends ListingTestCases[ObjectLocation, ObjectLocationPrefix, S3ObjectSummary, Bucket] with S3ListingFixtures {
  override def assertResultCorrect(result: Iterable[S3ObjectSummary], entries: Seq[ObjectLocation]): Assertion = {
    val actualLocations =
      result
        .toSeq
        .map { summary => ObjectLocation(summary.getBucketName, summary.getKey) }

    actualLocations should contain theSameElementsAs entries
  }

  override def withListing[R](bucket: Bucket, initialEntries: Seq[ObjectLocation])(testWith: TestWith[Listing[ObjectLocationPrefix, S3ObjectSummary], R]): R = {
    createInitialEntries(bucket, initialEntries)

    testWith(
      new S3ObjectSummaryListing()
    )
  }
}