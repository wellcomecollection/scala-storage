package uk.ac.wellcome.storage.transfer

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.generators.RandomThings
import uk.ac.wellcome.storage.listing.fixtures.ListingFixtures
import uk.ac.wellcome.storage.listing.memory.{MemoryListing, MemoryListingFixtures}
import uk.ac.wellcome.storage.store.Store
import uk.ac.wellcome.storage.store.memory.MemoryStore
import uk.ac.wellcome.storage.transfer.fixtures.TransferFixtures
import uk.ac.wellcome.storage.transfer.memory.{MemoryPrefixTransfer, MemoryTransfer, MemoryTransferFixtures}

trait PrefixTransferFixtures[Location, Prefix, T, StoreImpl <: Store[Location, T], TransferImpl <: Transfer[Location], StoreContext]
  extends TransferFixtures[Location, T, StoreImpl, TransferImpl, StoreContext]
    with ListingFixtures[Location, Prefix, Location, StoreContext] {
  def withPrefixTransferContext[R](testWith: TestWith[StoreContext, R]): R =
    withTransferStoreContext { context =>
      testWith(context)
    }

  override def withListingContext[R](testWith: TestWith[StoreContext, R]): R =
    withPrefixTransferContext { context =>
      testWith(context)
    }

  def withPrefixTransfer[R](initialEntries: Map[Location, T])(testWith: TestWith[PrefixTransfer[Prefix, Location], R])(implicit context: StoreContext): R
}

trait PrefixTransferTestCases[Location, Prefix, T, StoreImpl <: Store[Location, T], TransferImpl <: Transfer[Location], StoreContext] extends FunSpec with Matchers with PrefixTransferFixtures[Location, Prefix, T, StoreImpl, TransferImpl, StoreContext] {
  it("does nothing if the prefix is empty") {
    true shouldBe false
  }
}

class MemoryPrefixTransferTest extends
  PrefixTransferTestCases[String, String, Array[Byte], MemoryStore[String, Array[Byte]], MemoryTransfer[String, Array[Byte]], MemoryStore[String, Array[Byte]]] with MemoryListingFixtures[Array[Byte]]
with MemoryTransferFixtures[String, Array[Byte]] with RandomThings {
  override def createT: Array[Byte] = randomBytes()

  override def withPrefixTransfer[R](initialEntries: Map[String, Array[Byte]])(testWith: TestWith[PrefixTransfer[String, String], R])(implicit context: MemoryStore[String, Array[Byte]]): R =
    withTransfer { memoryTransfer =>
      withListing(context, initialEntries = Seq.empty) { memoryListing =>
        val prefixTransfer = new MemoryPrefixTransfer[String, String, Array[Byte]] {
          override implicit val transfer: MemoryTransfer[String, Array[Byte]] = memoryTransfer
          override implicit val listing: MemoryListing[String, String, Array[Byte]] = memoryListing

          override protected def buildDstLocation(srcPrefix: String, dstPrefix: String, srcLocation: String): String =
            srcLocation.replaceAll("^" + srcPrefix, dstPrefix)
        }

        testWith(prefixTransfer)
      }
    }
}
