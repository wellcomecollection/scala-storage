package uk.ac.wellcome.storage.transfer.memory

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.store.memory.MemoryStore
import uk.ac.wellcome.storage.transfer.fixtures.TransferFixtures

trait MemoryTransferFixtures[Ident, T] extends TransferFixtures[Ident, T, MemoryStore[Ident, T] with MemoryTransfer[Ident, T], MemoryTransfer[Ident, T]] {
  type MemoryTransferImpl = MemoryTransfer[Ident, T]
  type MemoryStoreImpl = MemoryStore[Ident, T] with MemoryTransferImpl

  override def withTransfer[R](testWith: TestWith[MemoryTransferImpl, R])(implicit store: MemoryStoreImpl): R =
    testWith(store)

  override def withTransferStore[R](initialEntries: Map[Ident, T])(testWith: TestWith[MemoryStoreImpl, R]): R = {
    val store = new MemoryStore[Ident, T](initialEntries) with MemoryTransfer[Ident, T]

    testWith(store)
  }
}
