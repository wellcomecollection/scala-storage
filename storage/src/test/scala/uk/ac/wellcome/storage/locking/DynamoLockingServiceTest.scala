package uk.ac.wellcome.storage.locking

import java.time.Instant
import java.util.UUID

import cats.implicits._
import com.gu.scanamo.Scanamo
import com.gu.scanamo.error.DynamoReadError
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{EitherValues, FunSpec}
import uk.ac.wellcome.monitoring.fixtures.MetricsSenderFixture
import uk.ac.wellcome.storage.UnlockFailure
import uk.ac.wellcome.storage.dynamo._
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table
import uk.ac.wellcome.storage.fixtures.{DynamoLockingFixtures, LocalDynamoDb}

import scala.collection.immutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.runtime.BoxedUnit
import scala.util.Random

class DynamoLockingServiceTest
  extends FunSpec
    with DynamoLockingFixtures
    with ScalaFutures
    with EitherValues
    with MetricsSenderFixture {

  def createTable(table: Table): Table = createLockTable(table)

  val id = Random.nextString(9)

  it("locks around a callback") {
    withLocalDynamoDbTable { lockTable =>
      withLockDao(lockTable) { dynamoRowLockDao =>
        withDynamoLockingService(dynamoRowLockDao) {
          lockingService =>

            val lockedDuringCallback =
              lockingService.withLocks(Set(id))(Future {
                assertOnlyHaveRowLockRecordIds(Set(id), lockTable)

                ()
              })

            whenReady(lockedDuringCallback) { _ =>
              assertNoLocks(lockTable)
            }
        }
      }
    }

  }

  it("doesn't call lock if lockIds is empty") {
    withLocalDynamoDbTable { lockTable =>
      val mockDynamoRowLockDao = mock[DynamoLockDao]
      withDynamoLockingService(mockDynamoRowLockDao) {
        lockingService =>

          var callbackCalled = false

          val lockedDuringCallback =
            lockingService.withLocks(Set.empty)(Future {
              callbackCalled = true
            })

          whenReady(lockedDuringCallback) { _ =>
            callbackCalled shouldBe true

            assertNoLocks(lockTable)
          }
      }
    }
  }

  it("fails and releases locks when a row lock fails") {
    withLocalDynamoDbTable { lockTable =>
      withLockDao(lockTable) { dynamoRowLockDao =>
        withDynamoLockingService(dynamoRowLockDao) {
          lockingService =>
            val idA = "id"
            val lockedId = "lockedId"

            givenLocks(ids = Set(lockedId), lockTable = lockTable)

            val eventuallyLockFails =
              lockingService.withLocks(Set(idA, lockedId))(Future {
                fail("BOOM!")
              })

            whenReady(eventuallyLockFails) { failure =>

              failure.left.value shouldBe a[FailedLock[_, _]]

              // still expect original locks to exist
              assertOnlyHaveRowLockRecordIds(
                Set(lockedId), lockTable
              )
            }
        }
      }

    }
  }

  it("releases locks when a nested row lock fails") {
    withLocalDynamoDbTable { lockTable =>
      withLockDao(lockTable) { dynamoRowLockDao =>
        withDynamoLockingService(dynamoRowLockDao) {
          lockingService =>

            val idA = "idA"
            val idB = "idB"

            givenLocks(ids = Set(idB), lockTable = lockTable)

            val nestedLock =
              lockingService.withLocks(Set(idA))(Future {
                lockingService.withLocks(Set(idB))(Future {
                  fail("BOOM!")
                })
              })

            whenReady(nestedLock) { lock =>
              lock.right.value shouldBe a[BoxedUnit]

              // still expect original locks to exist
              assertOnlyHaveRowLockRecordIds(Set(idB), lockTable)
            }
        }
      }
    }
  }

  it("returns a success if unlocking fails") {
    class BrokenUnlockDynamoLockDao(table: Table) extends DynamoLockDao(
      client = dynamoDbClient,
      config = DynamoLockDaoConfig(
        dynamoConfig = createDynamoConfigWith(table)
      )
    ) {
      override def unlock(contextId: ContextId): UnlockResult =
        Left(UnlockFailure(ctxId = contextId, e = new Throwable("BOOM!")))
    }

    withLocalDynamoDbTable { lockTable =>
      val brokenUnlockDao = new BrokenUnlockDynamoLockDao(lockTable)

      withDynamoLockingService(brokenUnlockDao) { lockingService =>
        val eventuallyLockFails =
          lockingService.withLocks(Set(id)) {
            Future.successful(Unit)
          }

        whenReady(eventuallyLockFails) { failure =>
          failure shouldBe a[Right[_, _]]
        }

        // Expect original locks to exist
        getDynamo(lockTable)(id).id shouldBe id
      }
    }
  }

  it("releases locks when the callback fails") {
    withLocalDynamoDbTable { lockTable =>
      withLockDao(lockTable) { dynamoRowLockDao =>
        withDynamoLockingService(dynamoRowLockDao) {
          lockingService =>
            case class ExpectedException() extends Exception()

            val eventuallyLockFails =
              lockingService.withLocks(Set(id))(Future {
                assertOnlyHaveRowLockRecordIds(Set(id), lockTable)
                fail("BOOM!")
              })

            whenReady(eventuallyLockFails) { failure =>
              failure.left.value shouldBe a[FailedProcess[_]]
              assertNoLocks(lockTable)
            }
        }

      }
    }
  }

  private def givenLocks(ids: Set[String],
                         contextId: UUID = createRandomContextId,
                         lockTable: LocalDynamoDb.Table): Unit =
    ids.foreach {
      id =>
        Scanamo.put[ExpiringLock](dynamoDbClient)(lockTable.name)(
          ExpiringLock(
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

    val locks: immutable.Seq[Either[DynamoReadError, ExpiringLock]] =
      Scanamo.scan[ExpiringLock](dynamoDbClient)(lockTable.name)

    val actualIds = locks.map(lock => lock.right.get.id).toSet
    actualIds shouldBe expectedIds
  }
}
