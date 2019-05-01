package uk.ac.wellcome.storage.fixtures

import grizzled.slf4j.Logging
import uk.ac.wellcome.storage.locking.ExpiringLock
import java.time.Duration
import java.util.UUID

import uk.ac.wellcome.storage.{Lock, LockDao, LockFailure}

trait LockDaoFixtures extends Logging {

  case class PermanentLock(id: String, contextId: UUID) extends Lock[String, UUID]

  def createBetterInMemoryLockDao: LockDao[String, UUID] = new LockDao[String, UUID] {
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

  def createInMemoryLockDao: LockDao[String, String] = new LockDao[String, String] {

    var locks: Map[String, ExpiringLock] = Map.empty

    private def createRowLock(id: String, ctxId: String) =
      ExpiringLock.create(id, ctxId, Duration.ofDays(1))

    override def unlock(ctxId: String): Right[Nothing, Unit] = {
      info(s"Unlocking for $ctxId")
      locks = locks.filter { case (id, ExpiringLock(_, ctx, _,_)) =>
        info(s"Unlocking $id")

        ctx != ctxId
      }

      Right(())
    }

    override def lock(id: String, ctxId: String): Either[LockFailure[String], ExpiringLock] = {
      info(s"Locking $id, with $ctxId")

      locks.get(id) match {
        case Some(r@ExpiringLock(_, ctx, _,_)) if ctxId == ctx => Right(r)
        case Some(ExpiringLock(_, ctx, _,_)) if ctxId != ctx => Left(
          LockFailure[String](id,
            new Throwable(
            s"Failed lock $id in $ctxId, locked: $ctx"
          ))
        )
        case None =>
          val rowLock = createRowLock(id, ctxId)
          locks = locks ++ Map(id -> rowLock)
          Right(rowLock)
      }
    }
  }
}
