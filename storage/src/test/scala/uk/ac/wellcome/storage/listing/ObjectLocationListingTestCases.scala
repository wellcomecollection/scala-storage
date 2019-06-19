package uk.ac.wellcome.storage.listing

import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.ObjectLocation
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
