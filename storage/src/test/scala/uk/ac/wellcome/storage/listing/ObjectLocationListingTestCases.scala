package uk.ac.wellcome.storage.listing

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}
import uk.ac.wellcome.storage.generators.{ObjectLocationGenerators, RandomThings}
import uk.ac.wellcome.storage.listing.memory.MemoryObjectLocationListing
import uk.ac.wellcome.storage.store.memory.MemoryStore

trait ListingFixtures[Ident, ListingContext] extends {
  def withListingContext[R](entries: Seq[Ident])(testWith: TestWith[ListingContext, R]): R

  def withListing[R](context: ListingContext)(testWith: TestWith[ObjectLocationListing, R]): R
}

trait ObjectLocationListingTestCases[ListingContext] extends FunSpec with Matchers with EitherValues with ObjectLocationGenerators with ListingFixtures[ObjectLocation, ListingContext] {
  describe("behaves as an object location listing") {
    it("doesn't find anything in an empty store") {
      withListingContext(entries = Seq.empty) { context =>
        withListing(context) { listing =>
          listing.list(createObjectLocationPrefix).right.value shouldBe empty
        }
      }
    }

    it("finds a single matching entry") {
      val location = createObjectLocation
      val prefix = location.asPrefix

      withListingContext(entries = Seq(location)) { context =>
        withListing(context) { listing =>
          listing.list(prefix).right.value.toSeq shouldBe Seq(location)
        }
      }
    }

    it("finds multiple matching entries") {
      val location = createObjectLocation
      val entries = Seq("1.txt", "2.txt", "3.txt").map { filename => location.join(filename) }
      val prefix = location.asPrefix

      withListingContext(entries = entries) { context =>
        withListing(context) { listing =>
          listing.list(prefix).right.value.toSeq should contain theSameElementsAs entries
        }
      }
    }

    it("ignores entries that don't match") {
      val location = createObjectLocation
      val entries = Seq("1.txt", "2.txt", "3.txt").map { filename => location.join(filename) }
      val prefix = location.asPrefix

      val extraEntries = Seq(createObjectLocation, createObjectLocation)

      withListingContext(entries = entries ++ extraEntries) { context =>
        withListing(context) { listing =>
          listing.list(prefix).right.value.toSeq should contain theSameElementsAs entries
        }
      }
    }

    it("ignores an entry where the key is a prefix but the namespace isn't") {
      val location = createObjectLocation
      val prefix = ObjectLocationPrefix(
        namespace = location.namespace + ".different",
        path = location.path + "/filename.txt"
      )

      withListingContext(entries = Seq(location)) { context =>
        withListing(context) { listing =>
          listing.list(prefix).right.value shouldBe empty
        }
      }
    }

    it("ignores an entry where the namespace matches but the key isn't a prefix") {
      val location = createObjectLocation
      val prefix = ObjectLocationPrefix(
        namespace = location.namespace,
        path = "directory/ " + location.path
      )

      withListingContext(entries = Seq(location)) { context =>
        withListing(context) { listing =>
          listing.list(prefix).right.value shouldBe empty
        }
      }
    }
  }
}

trait MemoryListingFixtures extends ListingFixtures[ObjectLocation, MemoryStore[ObjectLocation, String]] with RandomThings {
  override def withListingContext[R](entries: Seq[ObjectLocation])(testWith: TestWith[MemoryStore[ObjectLocation, String], R]): R = {
    val underlying = new MemoryStore[ObjectLocation, String](
      initialEntries = entries
        .map { loc => (loc, randomAlphanumeric) }
        .toMap
    )

    testWith(underlying)
  }

  override def withListing[R](context: MemoryStore[ObjectLocation, String])(testWith: TestWith[ObjectLocationListing, R]): R =
    testWith(
      new MemoryObjectLocationListing[String] {
        override var entries: Map[ObjectLocation, String] = context.entries
      }
    )
}

class MemoryObjectLocationListingTest extends ObjectLocationListingTestCases[MemoryStore[ObjectLocation, String]] with MemoryListingFixtures
