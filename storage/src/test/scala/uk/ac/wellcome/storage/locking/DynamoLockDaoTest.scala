package uk.ac.wellcome.storage.locking

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model._
import org.mockito.Matchers.any
import org.mockito.Mockito.when
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.storage.{LockFailure, UnlockFailure}
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table
import uk.ac.wellcome.storage.fixtures.DynamoLockingFixtures

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.runtime.BoxedUnit
import scala.util.Random

class DynamoLockDaoTest
  extends FunSpec
    with Matchers
    with MockitoSugar
    with ScalaFutures
    with DynamoLockingFixtures
    with EitherValues
    with IntegrationPatience {

  case class ThingToStore(id: String, value: String)

  private val staticId = "staticId"
  private val staticContextId = "staticContextId"

  def createTable(table: Table): Table =
    createLockTable(table)

  it("records a lock in DynamoDB") {
    withLocalDynamoDbTable { lockTable =>
      withLockDao(lockTable) { lockDao =>

        lockDao.lock(staticId, staticContextId)
          .right.value.id shouldBe staticId

        getDynamo(lockTable)(staticId)
          .id shouldBe staticId
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

        val expiry =
          getDynamo(lockTable)(staticId).expires

        // Wait at least 1 second
        Thread.sleep(1000)

        lockDao.lock(staticId, staticContextId)
          .right.value.id shouldBe staticId

        val updatedExpiry =
          getDynamo(lockTable)(staticId).expires

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

  describe("Locking problem:") {
    it("fails if there is a problem writing the lock") {
      val mockClient = mock[AmazonDynamoDB]

      val id = Random.nextString(9)

      val putItem = mockClient.putItem(any[PutItemRequest])
      val error = new InternalServerErrorException("FAILED")

      when(putItem).thenThrow(error)

      withLocalDynamoDbTable { lockTable =>
        withLockDao(mockClient, lockTable) { lockDao =>
          lockDao.lock(id, staticContextId)
            .left.value shouldBe LockFailure(id, error)
        }
      }
    }
  }

  describe("Unlocking problem:") {
    it("fails to read the context locks") {
      val mockClient = mock[AmazonDynamoDB]

      val contextId = Random.nextString(9)

      val query = mockClient.query(any[QueryRequest])
      val error = new InternalServerErrorException("FAILED")

      when(query).thenThrow(error)

      withLocalDynamoDbTable { lockTable =>
        withLockDao(mockClient, lockTable) { lockDao =>
          lockDao.unlock(contextId)
            .left.value shouldBe UnlockFailure(contextId, error)
        }
      }
    }

    it("fails to delete the lock") {
      val mockClient = mock[AmazonDynamoDB]

      val contextId = Random.nextString(9)

      val error = new InternalServerErrorException("FAILED")

      when(mockClient.query(any[QueryRequest]))
        .thenThrow(error)
      when(mockClient.deleteItem(any[DeleteItemRequest]))
        .thenThrow(error)

      withLocalDynamoDbTable { lockTable =>
        withLockDao(mockClient, lockTable) { lockDao =>
          lockDao.unlock(contextId)
            .left.value shouldBe UnlockFailure(contextId, error)
        }
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
