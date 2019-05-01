package uk.ac.wellcome.storage.fixtures

import grizzled.slf4j.Logging
import java.util.UUID

import uk.ac.wellcome.storage.{Lock, LockDao, LockFailure}

trait LockDaoFixtures extends Logging {

  case class PermanentLock(id: String, contextId: UUID) extends Lock[String, UUID]

  def createInMemoryLockDao: LockDao[String, UUID] = new LockDao[String, UUID] {
    var locks: Map[String, PermanentLock] = Map.empty

    override def lock(id: String, contextId: UUID): LockResult = {
      info(s"Locking ID <$id> in context <$contextId>")

      locks.get(id) match {
        case Some(r @ PermanentLock(_, existingContextId)) if contextId == existingContextId => Right(r)
        case Some(PermanentLock(_, existingContextId)) if contextId != existingContextId => Left(
          LockFailure[String](
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
          Right(rowLock)
      }
    }

    override def unlock(contextId: ContextId): UnlockResult = {
      info(s"Unlocking for context <$contextId>")
      locks = locks.filter { case (id, PermanentLock(_, lockContextId)) =>
        debug(s"Inspecting $id")
        contextId != lockContextId
      }

      Right(())
    }
  }
}
