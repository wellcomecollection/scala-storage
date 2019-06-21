package uk.ac.wellcome.storage.transfer.memory

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.store.memory.MemoryStore
import uk.ac.wellcome.storage.transfer.fixtures.TransferFixtures

trait MemoryTransferFixtures[Ident, T] extends TransferFixtures[Ident, T, MemoryStore[Ident, T] with MemoryTransfer[Ident, T], MemoryTransfer[Ident, T], MemoryStore[Ident, T] with MemoryTransfer[Ident, T]] {
  type MemoryTransferImpl = MemoryTransfer[Ident, T]
  type MemoryStoreImpl = MemoryStore[Ident, T] with MemoryTransferImpl

  override def withTransferStoreContext[R](testWith: TestWith[MemoryStoreImpl, R]): R =
    testWith(new MemoryStore[Ident, T](initialEntries = Map.empty) with MemoryTransfer[Ident, T])

  override def withTransferStore[R](initialEntries: Map[Ident, T])(testWith: TestWith[MemoryStoreImpl, R])(implicit store: MemoryStoreImpl): R = {
    store.entries = store.entries ++ initialEntries
    testWith(store)
  }

  override def withTransfer[R](testWith: TestWith[MemoryTransferImpl, R])(implicit underlying: MemoryStoreImpl): R =
    testWith(underlying)
}
