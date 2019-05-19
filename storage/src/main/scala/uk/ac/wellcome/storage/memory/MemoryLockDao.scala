package uk.ac.wellcome.storage.memory

import grizzled.slf4j.Logging
import uk.ac.wellcome.storage.{Lock, LockDao, LockFailure}

trait MemoryLockDao[MemoryIdent, MemoryContextId] extends LockDao[MemoryIdent, MemoryContextId] with Logging {
  type MemoryLock = PermanentLock[MemoryIdent, MemoryContextId]

  private var locks: Map[MemoryIdent, MemoryLock] = Map.empty

  var history: List[MemoryLock] = List.empty

  override def lock(id: MemoryIdent, contextId: MemoryContextId): LockResult = {
    info(s"Locking ID <$id> in context <$contextId>")

    locks.get(id) match {
      case Some(r @ PermanentLock(_, existingContextId)) if contextId == existingContextId => Right(r)
      case Some(PermanentLock(_, existingContextId)) if contextId != existingContextId => Left(
        LockFailure(
          id,
          new Throwable(s"Failed to lock <$id> in context <$contextId>; already locked as <$existingContextId>")
        )
      )
      case _ =>
        val rowLock = PermanentLock(
          id = id,
          contextId = contextId
        )
        locks = locks ++ Map(id -> rowLock)
        history = history :+ rowLock
        Right(rowLock)
    }
  }

  override def unlock(contextId: MemoryContextId): UnlockResult = {
    info(s"Unlocking for context <$contextId>")
    locks = locks.filter { case (id, PermanentLock(_, lockContextId)) =>
      debug(s"Inspecting $id")
      contextId != lockContextId
    }

    Right(())
  }

  def getCurrentLocks: Set[MemoryIdent] =
    locks.keys.toSet
}

case class PermanentLock[Ident, ContextId](id: Ident, contextId: ContextId) extends Lock[Ident, ContextId]
