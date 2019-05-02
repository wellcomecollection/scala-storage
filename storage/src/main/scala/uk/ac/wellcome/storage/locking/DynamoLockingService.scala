package uk.ac.wellcome.storage.locking

import java.util.UUID

import uk.ac.wellcome.storage.{LockDao, LockingService}

import scala.language.higherKinds

class DynamoLockingService[Out, OutMonad[_]](
  implicit val lockDao: DynamoLockDao)
    extends LockingService[Out, OutMonad, LockDao[String, UUID]] {
  override protected def createContextId(): lockDao.ContextId =
    UUID.randomUUID()
}
