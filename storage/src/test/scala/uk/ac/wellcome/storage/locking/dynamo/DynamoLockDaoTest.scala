package uk.ac.wellcome.storage.locking.dynamo

import java.util.UUID

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model._
import org.mockito.Matchers.any
import org.mockito.Mockito.when
import org.scalatest.concurrent.IntegrationPatience
import org.scalatest.mockito.MockitoSugar
import org.scanamo.auto._
import org.scanamo.time.JavaTimeFormats._
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table
import uk.ac.wellcome.storage.fixtures.DynamoLockingFixtures
import uk.ac.wellcome.storage.generators.RandomThings
import uk.ac.wellcome.storage.locking.{LockDao, LockDaoTestCases, LockFailure, UnlockFailure}

class DynamoLockDaoTest
  extends LockDaoTestCases[String, UUID, Table]
    with MockitoSugar
    with DynamoLockingFixtures
    with IntegrationPatience
    with RandomThings {

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

  private val staticId = createRandomId
  private val staticContextId = createRandomContextId

  it("records a lock in DynamoDB") {
    withLocalDynamoDbTable { lockTable =>
      withLockDao(lockTable) { lockDao =>

        lockDao.lock(staticId, staticContextId)
          .right.value.id shouldBe staticId

        getExistingTableItem[ExpiringLock](staticId, table = lockTable).id shouldBe staticId
      }
    }
  }

  it("refreshes the expiry on an existing lock") {
    withLocalDynamoDbTable { lockTable =>
      withLockDao(lockTable) { lockDao =>
        lockDao.lock(staticId, staticContextId)
          .right.value.id shouldBe staticId

        val expiry = getExistingTableItem[ExpiringLock](staticId, table = lockTable).expires

        // Wait at least 1 second
        Thread.sleep(1000)

        lockDao.lock(staticId, staticContextId)
          .right.value.id shouldBe staticId

        val updatedExpiry =
          getExistingTableItem[ExpiringLock](staticId, table = lockTable).expires

        expiry.isBefore(updatedExpiry) shouldBe true
      }
    }
  }

  it("creates a new lock in a different context when the existing lock expires") {
    withLocalDynamoDbTable { lockTable =>
      withLockDao(lockTable, seconds = 1) { lockDao =>
        val contextId = createRandomContextId

        lockDao.lock(staticId, staticContextId)
          .right.value.id shouldBe staticId

        lockDao.lock(staticId, contextId)
          .left.value shouldBe a[LockFailure[_]]

        // Allow the existing lock to expire
        Thread.sleep(2000)

        // Confirm we can lock expired lock
        lockDao.lock(staticId, contextId)
          .right.value.id shouldBe staticId
      }
    }
  }

  it("removes a lock from DynamoDB after unlocking") {
    withLocalDynamoDbTable { lockTable =>
      withLockDao(lockTable, seconds = 1) { lockDao =>
        lockDao.lock(staticId, staticContextId).right.value
        lockDao.unlock(staticContextId)
        assertNoLocks(lockTable)
      }
    }
  }

  describe("Locking problem:") {
    it("fails if there is a problem writing the lock") {
      val mockClient = mock[AmazonDynamoDB]

      val putItem = mockClient.putItem(any[PutItemRequest])
      val error = new InternalServerErrorException("FAILED")

      when(putItem).thenThrow(error)

      withLockDao(mockClient) { lockDao =>
        lockDao.lock(staticId, staticContextId)
          .left.value shouldBe LockFailure(staticId, error)
      }
    }
  }

  describe("Unlocking problem:") {
    it("fails to read the context locks") {
      val mockClient = mock[AmazonDynamoDB]

      val query = mockClient.query(any[QueryRequest])
      val error = new InternalServerErrorException("FAILED")

      when(query).thenThrow(error)

      withLockDao(mockClient) { lockDao =>
        lockDao.unlock(staticContextId)
          .left.value shouldBe UnlockFailure(staticContextId, error)
      }
    }

    it("fails to delete the lock") {
      val mockClient = mock[AmazonDynamoDB]

      val error = new InternalServerErrorException("FAILED")

      when(mockClient.query(any[QueryRequest]))
        .thenThrow(error)
      when(mockClient.deleteItem(any[DeleteItemRequest]))
        .thenThrow(error)

      withLockDao(mockClient) { lockDao =>
        lockDao.unlock(staticContextId)
          .left.value shouldBe UnlockFailure(staticContextId, error)
      }
    }
  }
}
