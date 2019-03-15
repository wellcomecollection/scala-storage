package uk.ac.wellcome.storage.locking

import java.time.Instant

import com.gu.scanamo.Scanamo
import com.gu.scanamo.error.DynamoReadError
import org.mockito.Matchers.any
import org.mockito.Mockito
import org.mockito.Mockito.{verify, verifyZeroInteractions, when}
import org.scalatest.{Assertion, FunSpec}
import org.scalatest.concurrent.ScalaFutures
import uk.ac.wellcome.monitoring.MetricsSender
import uk.ac.wellcome.monitoring.fixtures.MetricsSenderFixture
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table
import uk.ac.wellcome.storage.fixtures.{LocalDynamoDb, LockingFixtures}

import scala.collection.immutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class DynamoLockingServiceTest
    extends FunSpec
    with LockingFixtures
    with ScalaFutures
    with MetricsSenderFixture {

  def createTable(table: Table): Table = createLockTable(table)

  it("locks around a callback") {
    withMockMetricsSender { mockMetricsSender =>
      withLocalDynamoDbTable { lockTable =>
        withDynamoRowLockDao(lockTable) { dynamoRowLockDao =>
          withLockingService(dynamoRowLockDao, mockMetricsSender) {
            lockingService =>
              val id = "id"
              val lockedDuringCallback =
                lockingService.withLocks(Set(id))(Future {
                  assertOnlyHaveRowLockRecordIds(Set(id), lockTable)
                })

              whenReady(lockedDuringCallback) { _ =>
                assertNoRowLocks(lockTable)
                assertDoesNotIncrementFailedLockCount(mockMetricsSender)
              }
          }
        }
      }
    }
  }

  it("doesn't call lock if lockIds is empty") {
    withMockMetricsSender { mockMetricsSender =>
      withLocalDynamoDbTable { lockTable =>
        val mockDynamoRowLockDao = mock[DynamoRowLockDao]
        withLockingService(mockDynamoRowLockDao, mockMetricsSender) {
          lockingService =>
            var callbackCalled = false
            val lockedDuringCallback =
              lockingService.withLocks(Set.empty)(Future {
                callbackCalled = true
              })
            whenReady(lockedDuringCallback) { _ =>
              callbackCalled shouldBe true
              verifyZeroInteractions(mockDynamoRowLockDao)
              assertNoRowLocks(lockTable)
              assertDoesNotIncrementFailedLockCount(mockMetricsSender)
            }
        }
      }
    }
  }

  it("throws a FailedLockException and releases locks when a row lock fails") {
    withMockMetricsSender { mockMetricsSender =>
      withLocalDynamoDbTable { lockTable =>
        withDynamoRowLockDao(lockTable) { dynamoRowLockDao =>
          withLockingService(dynamoRowLockDao, mockMetricsSender) {
            lockingService =>
              val idA = "id"
              val lockedId = "lockedId"
              givenLocks(Set(lockedId), "existingContext", lockTable)

              val eventuallyLockFails =
                lockingService.withLocks(Set(idA, lockedId))(Future {
                  fail("Lock did not fail")
                })

              whenReady(eventuallyLockFails.failed) { failure =>
                failure shouldBe a[FailedLockException]
                // still expect original locks to exist
                assertOnlyHaveRowLockRecordIds(Set(lockedId), lockTable)
                assertIncrementsFailedLockCount(1, mockMetricsSender)
              }
          }
        }
      }
    }
  }

  it(
    "throws a FailedLockException and releases locks when a nested row lock fails") {
    withMockMetricsSender { mockMetricsSender =>
      withLocalDynamoDbTable { lockTable =>
        withDynamoRowLockDao(lockTable) { dynamoRowLockDao =>
          withLockingService(dynamoRowLockDao, mockMetricsSender) {
            lockingService =>
              val idA = "idA"
              val idB = "idB"
              givenLocks(Set(idB), "existingContext", lockTable)

              val eventuallyLockFails =
                lockingService.withLocks(Set(idA))({
                  lockingService.withLocks(Set(idB))(Future {
                    fail("Lock did not fail")
                  })
                })

              whenReady(eventuallyLockFails.failed) { failure =>
                failure shouldBe a[FailedLockException]
                // still expect original locks to exist
                assertOnlyHaveRowLockRecordIds(Set(idB), lockTable)
                assertIncrementsFailedLockCount(2, mockMetricsSender)
              }
          }
        }
      }
    }
  }

  it("throws a FailedUnlockException and releases locks when unlocking fails") {
    withMockMetricsSender { mockMetricsSender =>
      withLocalDynamoDbTable { lockTable =>
        val mockDynamoRowLockDao = mock[DynamoRowLockDao]
        withLockingService(mockDynamoRowLockDao, mockMetricsSender) {
          lockingService =>
            when(mockDynamoRowLockDao.unlockRows(any())).thenReturn(Future
              .failed(FailedUnlockException("Failed", new RuntimeException)))

            val eventuallyLockFails =
              lockingService.withLocks(Set("id"))(Future {})

            whenReady(eventuallyLockFails.failed) { failure =>
              failure shouldBe a[FailedUnlockException]
              // still expect original locks to exist
              assertNoRowLocks(lockTable)
              assertIncrementsFailedUnlockCount(1, mockMetricsSender)
            }
        }
      }
    }
  }

  it("releases locks when the callback fails") {
    withMockMetricsSender { mockMetricsSender =>
      withLocalDynamoDbTable { lockTable =>
        withDynamoRowLockDao(lockTable) { dynamoRowLockDao =>
          withLockingService(dynamoRowLockDao, mockMetricsSender) {
            lockingService =>
              case class ExpectedException() extends Exception()

              val id = "id"
              val eventuallyLockFails =
                lockingService.withLocks(Set(id))(Future {
                  assertOnlyHaveRowLockRecordIds(Set(id), lockTable)
                  throw new ExpectedException
                })

              whenReady(eventuallyLockFails.failed) { failure =>
                failure shouldBe a[ExpectedException]
                assertNoRowLocks(lockTable)
              }
          }
        }
      }
    }
  }

  private def givenLocks(ids: Set[String],
                         contextId: String,
                         lockTable: LocalDynamoDb.Table): Unit =
    ids.foreach {
      id =>
        Scanamo.put[RowLock](dynamoDbClient)(lockTable.name)(
          RowLock(
            id = id,
            contextId = contextId,
            created = Instant.now,
            expires = Instant.now.plusSeconds(100)
          )
        )
    }

  private def assertOnlyHaveRowLockRecordIds(
    expectedIds: Set[String],
    lockTable: LocalDynamoDb.Table): Any = {
    val locks: immutable.Seq[Either[DynamoReadError, RowLock]] =
      Scanamo.scan[RowLock](dynamoDbClient)(lockTable.name)
    val actualIds = locks.map(lock => lock.right.get.id).toSet
    actualIds shouldBe expectedIds
  }

  private def assertDoesNotIncrementFailedLockCount(
    mockMetricsSender: MetricsSender): Future[Unit] =
    verify(mockMetricsSender, Mockito.never()).incrementCount(any())

  private def assertIncrementsFailedLockCount(
    i: Int,
    mockMetricsSender: MetricsSender): Future[Any] = {
    verify(mockMetricsSender, Mockito.times(i))
      .incrementCount("WorkMatcher_FailedLock")
  }

  private def assertIncrementsFailedUnlockCount(
    i: Int,
    mockMetricsSender: MetricsSender): Future[Any] = {
    verify(mockMetricsSender, Mockito.times(i))
      .incrementCount("WorkMatcher_FailedUnlock")
  }

  def assertNoRowLocks(lockTable: LocalDynamoDb.Table): Assertion =
    Scanamo.scan[RowLock](dynamoDbClient)(lockTable.name) shouldBe empty
}
