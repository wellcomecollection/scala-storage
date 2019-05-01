package uk.ac.wellcome.storage.fixtures

import grizzled.slf4j.Logging
import uk.ac.wellcome.storage.locking.ExpiringLock
import java.time.Duration

import uk.ac.wellcome.storage.{LockDao, LockFailure}

trait LockDaoFixtures extends Logging {
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
