package uk.ac.wellcome.storage.locking.dynamo

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model._
import org.mockito.Matchers.any
import org.mockito.Mockito.when
import org.scalatest.FunSpec
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.mockito.MockitoSugar
import org.scanamo.auto._
import org.scanamo.time.JavaTimeFormats._
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table
import uk.ac.wellcome.storage.fixtures.DynamoLockingFixtures
import uk.ac.wellcome.storage.locking.{LockFailure, UnlockFailure}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.runtime.BoxedUnit

class DynamoLockDaoTest
  extends FunSpec
    with MockitoSugar
    with ScalaFutures
    with DynamoLockingFixtures
    with IntegrationPatience {

  private val staticId = createRandomId
  private val staticContextId = createRandomContextId

  def createTable(table: Table): Table =
    createLockTable(table)

  // Based on a similar test in InMemoryLockDaoTest, which is a high-level
  // check of the LockDao expectations.
  it("behaves correctly") {
    withLocalDynamoDbTable { lockTable =>
      withLockDao(lockTable) { lockDao =>
        val id1 = createRandomId
        val id2 = createRandomId

        val contextId1 = createRandomContextId
        val contextId2 = createRandomContextId

        lockDao.lock(id1, contextId1) shouldBe a[Right[_, _]]
        lockDao.lock(id2, contextId2) shouldBe a[Right[_, _]]

        lockDao.lock(id1, contextId2).left.value.e.getMessage should startWith(
          "The conditional request failed")

        lockDao.lock(id1, contextId1) shouldBe a[Right[_, _]]

        lockDao.unlock(contextId1) shouldBe a[Right[_, _]]
        lockDao.unlock(contextId1) shouldBe a[Right[_, _]]

        lockDao.lock(id1, contextId2) shouldBe a[Right[_, _]]
        lockDao.lock(id2, contextId1).left.value.e.getMessage should startWith(
          "The conditional request failed")
      }
    }
  }

  it("records a lock in DynamoDB") {
    withLocalDynamoDbTable { lockTable =>
      withLockDao(lockTable) { lockDao =>

        lockDao.lock(staticId, staticContextId)
          .right.value.id shouldBe staticId

        getExistingTableItem[ExpiringLock](staticId, table = lockTable).id shouldBe staticId
      }
    }
  }

  it("adds new locks to an existing context") {
    withLockDao { lockDao =>
      val id1 = createRandomId
      val id2 = createRandomId

      lockDao.lock(id1, staticContextId)
        .right.value.id shouldBe id1

      lockDao.lock(id2, staticContextId)
        .right.value.id shouldBe id2
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

  it("blocks locking the same ID in different contexts") {
    withLockDao { lockDao =>
      lockDao.lock(staticId, staticContextId)
        .right.value.id shouldBe staticId

      lockDao.lock(staticId, createRandomContextId)
        .left.value shouldBe a[LockFailure[_]]
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

  it("unlocks a locked context and can re-lock in a different context") {
    withLockDao { lockDao =>
      lockDao.lock(staticId, staticContextId)
        .right.value.id shouldBe staticId

      lockDao.unlock(staticContextId)
        .right.value shouldBe a[BoxedUnit]

      lockDao.lock(staticId, createRandomContextId)
        .right.value.id shouldBe staticId
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

  it("allows one success if multiple processes try to lock the same ID") {
    withLockDao { lockDao =>
      val lockUnlockCycles = 5
      val parallelism = 5

      // All locks/unlocks except one will fail in each cycle
      val expectedFail = parallelism - 1

      (1 to lockUnlockCycles).foreach { _ =>
        val id = createRandomId
        val countDownLatch = new CountDownLatch(parallelism)

        val eventualLocks = Future.sequence {
          (1 to parallelism).map { _ =>
            Future {
              countDownLatch.countDown()
              lockDao.lock(id, createRandomContextId)
            }
          }
        }

        countDownLatch.await(5, TimeUnit.SECONDS)

        whenReady(eventualLocks) { lockAttempts =>
          lockAttempts.count { _.isRight } shouldBe 1
          lockAttempts.count { _.isLeft } shouldBe expectedFail
        }
      }
    }
  }
}
