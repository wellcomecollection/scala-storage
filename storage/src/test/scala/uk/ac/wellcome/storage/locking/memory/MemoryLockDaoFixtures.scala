package uk.ac.wellcome.storage.locking.memory

import java.util.UUID

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.generators.RandomThings
import uk.ac.wellcome.storage.locking.{LockDao, LockDaoFixtures}

trait MemoryLockDaoFixtures
    extends LockDaoFixtures[String, UUID, Unit]
    with RandomThings {
  override def withLockDaoContext[R](testWith: TestWith[Unit, R]): R =
    testWith(())

  override def withLockDao[R](context: Unit)(
    testWith: TestWith[LockDao[String, UUID], R]): R =
    testWith(
      new MemoryLockDao[String, UUID]()
    )

  override def createIdent: String = randomAlphanumeric
  override def createContextId: UUID = UUID.randomUUID()
}
