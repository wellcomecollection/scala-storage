package uk.ac.wellcome.storage.locking.dynamo

import cats.implicits._
import org.scalatest.FunSpec
import org.scalatest.concurrent.ScalaFutures
import org.scanamo.auto._
import org.scanamo.time.JavaTimeFormats._
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table
import uk.ac.wellcome.storage.fixtures.{DynamoFixtures, DynamoLockingFixtures}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class DynamoLockingServiceTest
  extends FunSpec
    with DynamoLockingFixtures
    with ScalaFutures {

  def createTable(table: Table): Table = createLockTable(table)

  val id = createRandomId

  it("records locks in DynamoDB") {
    withLocalDynamoDbTable { lockTable =>
      withDynamoLockingService(lockTable) { lockingService =>

        val lockedDuringCallback =

          // Acquire a lock, and check that it's
          // recorded in DynamoDB
          lockingService.withLocks(Set("1", "2")) { Future {
            assertDynamoHasLockIds(Set("1", "2"), lockTable)

            ()
          }}

        whenReady(lockedDuringCallback) { _ =>
          assertNoLocks(lockTable)
        }
      }
    }
  }

  private def assertDynamoHasLockIds(
    expectedIds: Set[String],
    lockTable: DynamoFixtures.Table): Any = {
    val storedIds =
      scanTable[ExpiringLock](lockTable)
        .map { lock => lock.right.value }
        .toSet

    storedIds shouldBe expectedIds
  }
}
