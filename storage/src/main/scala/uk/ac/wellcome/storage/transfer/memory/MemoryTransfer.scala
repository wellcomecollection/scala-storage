package uk.ac.wellcome.storage.transfer.memory

import uk.ac.wellcome.storage.store.memory.MemoryStoreBase
import uk.ac.wellcome.storage.transfer._

trait MemoryTransfer[Ident, T]
    extends Transfer[Ident]
    with MemoryStoreBase[Ident, T] {
  override def transferWithCheckForExisting(src: Ident, dst: Ident): Either[TransferFailure, TransferSuccess] =
    (entries.get(src), entries.get(dst)) match {
      case (Some(srcT), Some(dstT)) if srcT == dstT =>
        Right(TransferNoOp(src, dst))
      case (Some(_), Some(_)) =>
        Left(TransferOverwriteFailure(src, dst))
      case (Some(srcT), _) =>
        entries = entries ++ Map(dst -> srcT)
        Right(TransferPerformed(src, dst))
      case (None, _) =>
        Left(TransferSourceFailure(src, dst))
    }

  override def transferWithOverwrites(src: Ident, dst: Ident): Either[TransferFailure, TransferSuccess] =
    entries.get(src) match {
      case Some(srcT) =>
        entries = entries ++ Map(dst -> srcT)
        Right(TransferPerformed(src, dst))

      case None =>
        Left(TransferSourceFailure(src, dst))
    }
}
