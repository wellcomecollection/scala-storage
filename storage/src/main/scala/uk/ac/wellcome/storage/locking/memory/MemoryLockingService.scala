package uk.ac.wellcome.storage.locking.memory

import java.util.UUID

import uk.ac.wellcome.storage.locking.{LockDao, LockingService}

import scala.language.higherKinds

class MemoryLockingService[Out, OutMonad[_]](implicit val lockDao: MemoryLockDao[String, UUID])
  extends LockingService[Out, OutMonad, LockDao[String, UUID]] {
  override protected def createContextId(): lockDao.ContextId =
    UUID.randomUUID()
}
