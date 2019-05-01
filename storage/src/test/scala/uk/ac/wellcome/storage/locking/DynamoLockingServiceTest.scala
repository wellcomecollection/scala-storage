package uk.ac.wellcome.storage.locking

import cats.implicits._
import com.gu.scanamo.Scanamo
import com.gu.scanamo.error.DynamoReadError
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.FunSpec
import uk.ac.wellcome.storage.dynamo._
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table
import uk.ac.wellcome.storage.fixtures.{DynamoLockingFixtures, LocalDynamoDb}

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

          // Acquire an initial lock, and check that it's
          // recorded in DynamoDB
          lockingService.withLocks(Set("1")) { Future {
            assertDynamoHasLockIds(Set("1"), lockTable)

            lockingService.withLocks(Set("2", "3")) { Future {
              assertDynamoHasLockIds(Set("1", "2", "3"), lockTable)
            }}

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
    lockTable: LocalDynamoDb.Table): Any = {
    val locks: Seq[Either[DynamoReadError, ExpiringLock]] =
      Scanamo.scan[ExpiringLock](dynamoDbClient)(lockTable.name)

    val actualIds = locks.map(lock => lock.right.get.id).toSet
    actualIds shouldBe expectedIds
  }
}
