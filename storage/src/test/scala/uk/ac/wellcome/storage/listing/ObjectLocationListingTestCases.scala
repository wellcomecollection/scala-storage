package uk.ac.wellcome.storage.listing

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.{ObjectLocation, ObjectLocationPrefix}
import uk.ac.wellcome.storage.generators.{ObjectLocationGenerators, RandomThings}
import uk.ac.wellcome.storage.listing.memory.MemoryObjectLocationListing
import uk.ac.wellcome.storage.store.memory.MemoryStore

trait ObjectLocationListingTestCases[StoreContext] extends FunSpec with Matchers with EitherValues with ObjectLocationGenerators {
  def withStoreContext[R](entries: Seq[ObjectLocation])(testWith: TestWith[StoreContext, R]): R

  def withListing[R](context: StoreContext)(testWith: TestWith[ObjectLocationListing, R]): R

  describe("behaves as an object location listing") {
    it("doesn't find anything in an empty store") {
      withStoreContext(entries = Seq.empty) { context =>
        withListing(context) { listing =>
          listing.list(createObjectLocationPrefix).right.value shouldBe empty
        }
      }
    }

    it("finds a single matching entry") {
      val location = createObjectLocation
      val prefix = location.asPrefix

      withStoreContext(entries = Seq(location)) { context =>
        withListing(context) { listing =>
          listing.list(prefix).right.value.toSeq shouldBe Seq(location)
        }
      }
    }

    it("finds multiple matching entries") {
      val location = createObjectLocation
      val entries = Seq("1.txt", "2.txt", "3.txt").map { filename => location.join(filename) }
      val prefix = location.asPrefix

      withStoreContext(entries = entries) { context =>
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

      withStoreContext(entries = entries ++ extraEntries) { context =>
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

      withStoreContext(entries = Seq(location)) { context =>
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

      withStoreContext(entries = Seq(location)) { context =>
        withListing(context) { listing =>
          listing.list(prefix).right.value shouldBe empty
        }
      }
    }
  }
}

class MemoryObjectLocationListingTest extends ObjectLocationListingTestCases[MemoryStore[ObjectLocation, String]] with RandomThings {
  override def withStoreContext[R](entries: Seq[ObjectLocation])(testWith: TestWith[MemoryStore[ObjectLocation, String], R]): R = {
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
