package uk.ac.wellcome.storage.transfer

import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.storage.generators.RandomThings
import uk.ac.wellcome.storage.listing.fixtures.ListingFixtures
import uk.ac.wellcome.storage.listing.memory.MemoryListingFixtures
import uk.ac.wellcome.storage.store.Store
import uk.ac.wellcome.storage.store.memory.MemoryStore
import uk.ac.wellcome.storage.transfer.fixtures.TransferFixtures
import uk.ac.wellcome.storage.transfer.memory.MemoryTransferFixtures

trait PrefixTransferFixtures[Ident, Prefix, T, StoreImpl <: Store[Ident, T], StoreContext]
  extends TransferFixtures[Ident, T, StoreImpl, StoreContext]
    with ListingFixtures[Ident, Prefix, Ident, StoreContext]

trait PrefixTransferTestCases[Ident, Prefix, T, StoreImpl <: Store[Ident, T], StoreContext] extends FunSpec with Matchers with PrefixTransferFixtures[Ident, Prefix, T, StoreImpl, StoreContext] {
  it("does nothing if the prefix is empty") {
    true shouldBe false
  }
}

class MemoryPrefixTransferTest extends
  PrefixTransferTestCases[String, String, Array[Byte], MemoryStore[String, Array[Byte]], MemoryStore[String, Array[Byte]]] with MemoryListingFixtures[Array[Byte]]
with MemoryTransferFixtures[String, Array[Byte]] with RandomThings {
  override def createT: Array[Byte] = randomBytes()
}
