package uk.ac.wellcome.storage.locking

import java.util.UUID

import uk.ac.wellcome.storage.LockingService

import scala.concurrent.Future

class DynamoLockingService(implicit val lockDao: DynamoLockDao)
    extends LockingService[Unit, Future, DynamoLockDao] {
  override protected def createContextId(): lockDao.ContextId =
    UUID.randomUUID()
}
