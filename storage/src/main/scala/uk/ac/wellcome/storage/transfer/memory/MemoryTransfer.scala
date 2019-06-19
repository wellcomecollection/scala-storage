package uk.ac.wellcome.storage.transfer.memory

import uk.ac.wellcome.storage.store.memory.MemoryStore
import uk.ac.wellcome.storage.transfer._

class MemoryTransfer[Ident, T](underlying: MemoryStore[Ident, T]) extends Transfer[Ident] {
  override def transfer(src: Ident, dst: Ident): Either[TransferFailure, TransferSuccess] =
    (underlying.get(src), underlying.get(dst)) match {
      case (Right(srcT), Right(dstT)) if srcT.identifiedT == dstT.identifiedT =>
        Right(TransferPerformed(src, dst))
      case (Right(_), Right(_)) =>
        Left(TransferOverwriteFailure(src, dst))
      case (Right(srcT), _) => put(src, dst, srcT.identifiedT)
      case (Left(err), _) => Left(TransferSourceFailure(src, dst, err.e))
    }

  private def put(src: Ident, dst: Ident, t: T): Either[TransferDestinationFailure[Ident], TransferPerformed[Ident]] =
    underlying.put(dst)(t) match {
      case Right(_) => Right(TransferPerformed(src, dst))
      case Left(err) => Left(TransferDestinationFailure(src, dst, err.e))
    }
}
