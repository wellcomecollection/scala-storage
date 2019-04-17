package uk.ac.wellcome.storage.fixtures

import grizzled.slf4j.Logging
import uk.ac.wellcome.storage.locking.{LockDao, LockFailure, UnlockFailure}


trait LockDaoFixtures extends Logging {
  def createInMemoryLockDao = new LockDao[String, String, Unit] {

    var locks: Map[String, String] = Map.empty

    override def lock(id: String, ctxId: String): Either[LockFailure[String], Unit] = {
      info(s"Locking $id, with $ctxId")

      locks.get(id) match {
        case Some(existingCtxId) if ctxId == existingCtxId => Right(())
        case Some(existingCtxId) if ctxId != existingCtxId => Left(
          LockFailure[String](id, new Throwable(
              s"Unable to lock $id in $ctxId, already locked with context $existingCtxId"
          ))
        )
        case None =>
          locks = locks ++ Map(id -> ctxId)
          Right(())
      }
    }

    override def unlock(ctxId: String): Right[Nothing, Unit] = {
      info(s"Unlocking for $ctxId")
      locks = locks.filter { case (id, v) =>
        info(s"Unlocking $id")

        v != ctxId
      }

      Right(())
    }

    override def handleUnlockError(failed: UnlockFailure[String, String]): Unit = {
      info(s"Handling unlock error: $failed")
      super.handleUnlockError(failed)
    }
  }
}
