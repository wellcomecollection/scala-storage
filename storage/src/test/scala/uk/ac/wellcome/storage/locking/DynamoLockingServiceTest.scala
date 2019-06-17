package uk.ac.wellcome.storage.locking

import cats.implicits._
import com.gu.scanamo.Scanamo
import com.gu.scanamo.error.DynamoReadError
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.FunSpec
import uk.ac.wellcome.storage.dynamo._
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table
import uk.ac.wellcome.storage.fixtures.{DynamoLockingFixtures, DynamoFixtures}

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
    val locks: Seq[Either[DynamoReadError, ExpiringLock]] =
      Scanamo.scan[ExpiringLock](dynamoClient)(lockTable.name)

    val actualIds = locks.map(lock => lock.right.get.id).toSet
    actualIds shouldBe expectedIds
  }
}
