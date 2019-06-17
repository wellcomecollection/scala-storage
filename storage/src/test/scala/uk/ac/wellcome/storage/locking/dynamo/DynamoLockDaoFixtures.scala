package uk.ac.wellcome.storage.locking.dynamo

import java.util.UUID

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table
import uk.ac.wellcome.storage.fixtures.DynamoLockingFixtures
import uk.ac.wellcome.storage.generators.RandomThings
import uk.ac.wellcome.storage.locking.{LockDao, LockDaoFixtures}

trait DynamoLockDaoFixtures extends LockDaoFixtures[String, UUID, Table] with DynamoLockingFixtures with RandomThings {
  def createTable(table: Table): Table =
    createLockTable(table)

  override def withLockDaoContext[R](testWith: TestWith[Table, R]): R =
    withLocalDynamoDbTable { table =>
      testWith(table)
    }

  override def withLockDao[R](lockTable: Table)(testWith: TestWith[LockDao[String, UUID], R]): R =
    withLockDao(dynamoClient, lockTable = lockTable) { lockDao =>
      testWith(lockDao)
    }

  override def createIdent: String = randomAlphanumeric
  override def createContextId: UUID = UUID.randomUUID()
}
