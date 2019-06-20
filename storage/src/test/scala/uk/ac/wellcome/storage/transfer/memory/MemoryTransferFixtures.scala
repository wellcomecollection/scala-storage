package uk.ac.wellcome.storage.transfer.memory

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.store.memory.MemoryStore
import uk.ac.wellcome.storage.transfer.Transfer
import uk.ac.wellcome.storage.transfer.fixtures.TransferFixtures

trait MemoryTransferFixtures[Ident, T] extends TransferFixtures[Ident, T, MemoryStore[Ident, T], MemoryTransfer[Ident, T], MemoryStore[Ident, T]] {
  override def withTransferStoreContext[R](testWith: TestWith[MemoryStore[Ident, T], R]): R =
    testWith(new MemoryStore[Ident, T](initialEntries = Map.empty))

  override def withTransferStore[R](initialEntries: Map[Ident, T])(testWith: TestWith[MemoryStore[Ident, T], R])(implicit store: MemoryStore[Ident, T]): R = {
    store.entries = store.entries ++ initialEntries
    testWith(store)
  }

  override def withTransfer[R](testWith: TestWith[MemoryTransfer[Ident, T], R])(implicit underlying: MemoryStore[Ident, T]): R =
    testWith(new MemoryTransfer[Ident, T](underlying))
}
