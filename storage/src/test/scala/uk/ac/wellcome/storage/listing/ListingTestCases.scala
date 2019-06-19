package uk.ac.wellcome.storage.listing

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.generators.{ObjectLocationGenerators, RandomThings}
import uk.ac.wellcome.storage.listing.memory.MemoryListing
import uk.ac.wellcome.storage.store.memory.MemoryStore

trait ListingFixtures[Ident, Prefix, ListingContext] extends {
  def withListingContext[R](entries: Seq[Ident])(testWith: TestWith[ListingContext, R]): R

  def withListing[R](context: ListingContext)(testWith: TestWith[Listing[Ident, Prefix], R]): R
}

trait ListingTestCases[Ident, Prefix, ListingContext] extends FunSpec with Matchers with EitherValues with ObjectLocationGenerators with ListingFixtures[Ident, Prefix, ListingContext] {
  def createIdent: Ident
  def extendIdent(id: Ident, extension: String): Ident
  def createPrefix: Prefix
  def createPrefixMatching(id: Ident): Prefix

  describe("behaves as an object location listing") {
    it("doesn't find anything in an empty store") {
      withListingContext(entries = Seq.empty) { context =>
        withListing(context) { listing =>
          listing.list(createPrefix).right.value shouldBe empty
        }
      }
    }

    it("finds a single entry where the prefix matches the ident") {
      val ident = createIdent
      val prefix = createPrefixMatching(ident)

      withListingContext(entries = Seq(ident)) { context =>
        withListing(context) { listing =>
          listing.list(prefix).right.value.toSeq shouldBe Seq(ident)
        }
      }
    }

    it("finds a single entry where the prefix is strictly shorter than the ident") {
      val ident = createIdent
      val prefix = createPrefixMatching(ident)
      val entries = Seq(extendIdent(ident, randomAlphanumeric))

      withListingContext(entries = entries) { context =>
        withListing(context) { listing =>
          listing.list(prefix).right.value.toSeq shouldBe entries
        }
      }
    }

    it("finds multiple matching entries") {
      val ident = createIdent
      val entries = Seq("1.txt", "2.txt", "3.txt").map { filename => extendIdent(ident, filename) }
      val prefix = createPrefixMatching(ident)

      withListingContext(entries = entries) { context =>
        withListing(context) { listing =>
          listing.list(prefix).right.value.toSeq should contain theSameElementsAs entries
        }
      }
    }

    it("ignores entries that don't match") {
      val ident = createIdent
      val entries = Seq("1.txt", "2.txt", "3.txt").map { filename => extendIdent(ident, filename) }
      val prefix = createPrefixMatching(ident)

      val extraEntries = Seq(createIdent, createIdent)

      withListingContext(entries = entries ++ extraEntries) { context =>
        withListing(context) { listing =>
          listing.list(prefix).right.value.toSeq should contain theSameElementsAs entries
        }
      }
    }
  }
}

trait MemoryListingFixtures[T] extends ListingFixtures[String, String, MemoryStore[String, T]] {
  def createT: T

  override def withListingContext[R](entries: Seq[String])(testWith: TestWith[MemoryStore[String, T], R]): R = {
    val underlying = new MemoryStore[String, T](
      initialEntries = entries
        .map { loc => (loc, createT) }
        .toMap
    )

    testWith(underlying)
  }

  override def withListing[R](context: MemoryStore[String, T])(testWith: TestWith[Listing[String, String], R]): R =
    testWith(
      new MemoryListing[String, String, T] {
        override var entries: Map[String, T] = context.entries

        override protected def startsWith(id: String, prefix: String): Boolean = id.startsWith(prefix)
      }
    )
}

class MemoryListingTest extends ListingTestCases[String, String, MemoryStore[String, Array[Byte]]] with MemoryListingFixtures[Array[Byte]] with RandomThings {
  def createT: Array[Byte] = randomBytes()

  override def createIdent: String = randomAlphanumeric

  override def extendIdent(id: String, extension: String): String = id + extension

  override def createPrefix: String = randomAlphanumeric

  override def createPrefixMatching(id: String): String = id
}
