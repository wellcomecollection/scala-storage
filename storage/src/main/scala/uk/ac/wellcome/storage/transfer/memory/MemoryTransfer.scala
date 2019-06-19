package uk.ac.wellcome.storage.transfer.memory

import uk.ac.wellcome.storage.store.memory.MemoryStore
import uk.ac.wellcome.storage.transfer._

class MemoryTransfer[Ident, T](underlying: MemoryStore[Ident, T]) extends Transfer[Ident, Ident] {
  override def transfer(src: Ident, dst: Ident): Either[TransferFailure, TransferSuccess] = {
    underlying.entries = underlying.entries ++ Map(dst -> underlying.entries(src))
    Right(TransferPerformed(src, dst))
  }
}
