package uk.ac.wellcome.storage.locking

import java.time.{Duration, Instant}
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model._
import com.gu.scanamo._
import org.mockito.Matchers.any
import org.mockito.Mockito.when
import org.scalatest.concurrent.{PatienceConfiguration, ScalaFutures}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table
import uk.ac.wellcome.storage.fixtures.LockingFixtures

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.runtime.BoxedUnit
import scala.util.Random

class DynamoLockDaoTest
  extends FunSpec
    with Matchers
    with MockitoSugar
    with ScalaFutures
    with LockingFixtures
    with EitherValues
    with PatienceConfiguration {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(
    timeout = scaled(Span(40, Seconds)),
    interval = scaled(Span(150, Millis))
  )

  import com.gu.scanamo.syntax._

  case class ThingToStore(id: String, value: String)

  private val contextId = Random.nextString(9)

  def createTable(table: Table): Table =
    createLockTable(table)

  it("locks a thing") {
    withLocalDynamoDbTable { lockTable =>
      withLockDao(lockTable) { lockDao =>
        val id = Random.nextString(32)

        lockDao.lock(id, contextId)
          .right.value.id shouldBe id

        val actualStored =
          Scanamo.get[RowLock](
            dynamoDbClient
          )(
            lockTable.name
          )('id -> id)

        actualStored.get match {
          case Right(storedRowLock) =>
            storedRowLock.id shouldBe id
          case Left(failed) =>
            fail(s"failed to get rowLocks $failed")
        }
      }
    }
  }

  it("cannot lock a locked thing") {
    withLocalDynamoDbTable { lockTable =>
      withLockDao(lockTable) { lockDao =>
        val id = Random.nextString(32)

        Scanamo.put[RowLock](dynamoDbClient)(
          lockTable.name
        )(
          RowLock(
            id, contextId,
            Instant.now,
            Instant.now.plusSeconds(100)
          )
        )


        lockDao.lock(id, contextId)
          .left.value shouldBe a[LockFailure[_]]
      }
    }
  }

  it("can lock a locked thing that has expired") {
    withLocalDynamoDbTable { lockTable =>
      withLockDao(lockTable) { lockDao =>
        val id = Random.nextString(32)

        lockDao.lock(id, contextId)
          .right.value.id shouldBe id

        lockDao.lock(id, contextId)
          .left.value shouldBe a[LockFailure[_]]

        // Expire stored lock
        val actualStored =
          Scanamo.get[RowLock](
            dynamoDbClient
          )(
            lockTable.name
          )('id -> id)

        val rowLock = actualStored.get.right.get

        val expiryTimeInThePast =
          Instant.now()
            .minus(
              Duration.ofSeconds(1)
            )

        val updatedRowLock =
          rowLock.copy(
            expires = expiryTimeInThePast
          )

        Scanamo.put[RowLock](
          dynamoDbClient
        )(
          lockTable.name
        )(updatedRowLock)

        // Confirm we can lock expired lock
        lockDao.lock(id, contextId)
          .right.value.id shouldBe id
      }
    }
  }

  it("unlocks a locked thing and can lock it again") {
    withLocalDynamoDbTable { lockTable =>
      withLockDao(lockTable) { lockDao =>
        val id = Random.nextString(32)

        lockDao.lock(id, contextId)
          .right.value.id shouldBe id

        lockDao.unlock(contextId)
          .right.value shouldBe a[BoxedUnit]

        lockDao.lock(id, contextId)
          .right.value.id shouldBe id

      }
    }
  }

  it("fails if there is a problem writing the lock") {
    val mockClient = mock[AmazonDynamoDB]

    val id = Random.nextString(9)

    val putItem = mockClient.putItem(any[PutItemRequest])
    val error = new InternalServerErrorException("FAILED")

    when(putItem).thenThrow(error)

    withLocalDynamoDbTable { lockTable =>
      withLockDao(mockClient, lockTable) { lockDao =>

        lockDao.lock(id, contextId)
          .left.value shouldBe LockFailure(id, error)

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

  it("allows one success if multiple processes lock a thing") {
    withLocalDynamoDbTable { lockTable =>
      withLockDao(lockTable) { lockDao =>
        val id = Random.nextString(32)

        val lockUnlockCycles = 5
        val parallelism = 5

        // All locks/unlocks except one will fail in each cycle
        val expectedFail = parallelism - 1

        (1 to lockUnlockCycles).foreach { _ =>

          val countDownLatch = new CountDownLatch(parallelism)

          val eventualLocks = Future.sequence {
            (1 to parallelism).map { _ =>
              Future {
                countDownLatch.countDown()
                lockDao.lock(id, contextId)
              }
            }
          }

          countDownLatch.await(5, TimeUnit.SECONDS)

          whenReady(eventualLocks) { lockAttempts =>

            lockAttempts.collect {
              case Right(a) => a
            }.size shouldBe 1

            lockAttempts.collect {
              case Left(a) => a
            }.size shouldBe expectedFail
          }

          lockDao.unlock(contextId) shouldBe a[Right[_,_]]
        }
      }
    }
  }
}
