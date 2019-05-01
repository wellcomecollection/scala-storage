package uk.ac.wellcome.storage.locking

import java.time.Instant
import java.util.UUID

import cats.implicits._
import com.gu.scanamo.Scanamo
import com.gu.scanamo.error.DynamoReadError
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{EitherValues, FunSpec}
import uk.ac.wellcome.monitoring.fixtures.MetricsSenderFixture
import uk.ac.wellcome.storage.FailedLock
import uk.ac.wellcome.storage.dynamo._
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table
import uk.ac.wellcome.storage.fixtures.{DynamoLockingFixtures, LocalDynamoDb}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.runtime.BoxedUnit

class DynamoLockingServiceTest
  extends FunSpec
    with DynamoLockingFixtures
    with ScalaFutures
    with EitherValues
    with MetricsSenderFixture {

  def createTable(table: Table): Table = createLockTable(table)

  val id = createRandomId

  it("records locks in DynamoDB") {
    withLocalDynamoDbTable { lockTable =>
      withDynamoLockingService(lockTable) { lockingService =>

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

  it("fails and releases locks when a row lock fails") {
    withLocalDynamoDbTable { lockTable =>
      withDynamoLockingService(lockTable) { lockingService =>
        val idA = "id"
        val lockedId = "lockedId"

        givenLocks(ids = Set(lockedId), lockTable = lockTable)

        // This should fail because it can't acquire a lock on `lockedId`
        val eventuallyLockFails =
          lockingService.withLocks(Set(idA, lockedId))(
            Future.successful(Unit)
          )

        whenReady(eventuallyLockFails) { failure =>
          failure.left.value shouldBe a[FailedLock[_, _]]

          // The original lock should be preserved
          assertOnlyHaveRowLockRecordIds(
            Set(lockedId), lockTable
          )
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
                  Future.successful(Unit)
                })
              })

            whenReady(nestedLock) { lock =>
              lock.right.value shouldBe a[BoxedUnit]

              // The original lock should be preserved
              assertOnlyHaveRowLockRecordIds(Set(idB), lockTable)
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
    val locks: Seq[Either[DynamoReadError, ExpiringLock]] =
      Scanamo.scan[ExpiringLock](dynamoDbClient)(lockTable.name)

    val actualIds = locks.map(lock => lock.right.get.id).toSet
    actualIds shouldBe expectedIds
  }
}
