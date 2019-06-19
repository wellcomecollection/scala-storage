package uk.ac.wellcome.storage.transfer.memory

import uk.ac.wellcome.storage.store.memory.MemoryStore
import uk.ac.wellcome.storage.transfer._

class MemoryTransfer[Ident, T](underlying: MemoryStore[Ident, T]) extends Transfer[Ident] {
  override def transfer(src: Ident, dst: Ident): Either[TransferFailure, TransferSuccess] =
    underlying.get(src) match {
      case Right(t) => underlying.put(dst)(t.identifiedT) match {
        case Right(_) => Right(TransferPerformed(src, dst))
        case Left(err) => Left(TransferDestinationFailure(src, dst, err.e))
      }
      case Left(err) => Left(TransferSourceFailure(src, dst, err.e))
    }
}
