package uk.ac.wellcome.storage.locking.dynamo

import java.util.UUID

import org.scalatest.EitherValues
import org.scanamo.auto._
import org.scanamo.time.JavaTimeFormats._
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table
import uk.ac.wellcome.storage.locking.LockingServiceTestCases

class DynamoLockingServiceTest extends LockingServiceTestCases[String, UUID, Table] with DynamoLockDaoFixtures with EitherValues {
  override def getCurrentLocks(lockDao: LockDaoStub, lockDaoContext: Table): Set[String] =
    scanTable[ExpiringLock](lockDaoContext)
      .map { _.right.value }
      .map { _.id }
      .toSet
}
