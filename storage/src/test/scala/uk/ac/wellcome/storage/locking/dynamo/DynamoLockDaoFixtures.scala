package uk.ac.wellcome.storage.locking.dynamo

import java.time.Duration
import java.util.UUID

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.util.TableUtils.waitUntilActive
import org.scalatest.Assertion
import org.scanamo.auto._
import org.scanamo.time.JavaTimeFormats._
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.fixtures.DynamoFixtures
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table
import uk.ac.wellcome.storage.generators.RandomThings
import uk.ac.wellcome.storage.locking.{LockDao, LockDaoFixtures}

import scala.concurrent.ExecutionContext.Implicits.global

trait DynamoLockDaoFixtures
    extends LockDaoFixtures[String, UUID, Table]
    with DynamoFixtures
    with RandomThings {
  def createTable(table: Table): Table =
    createLockTable(table)

  override def withLockDaoContext[R](testWith: TestWith[Table, R]): R =
    withLocalDynamoDbTable { table =>
      testWith(table)
    }

  override def withLockDao[R](lockTable: Table)(
    testWith: TestWith[LockDao[String, UUID], R]): R =
    withLockDao(dynamoClient, lockTable = lockTable) { lockDao =>
      testWith(lockDao)
    }

  override def createIdent: String = randomAlphanumeric
  override def createContextId: UUID = UUID.randomUUID()

  def assertNoLocks(lockTable: Table): Assertion =
    scanTable[ExpiringLock](lockTable) shouldBe empty

  def withLockDao[R](
    dynamoClient: AmazonDynamoDB,
    lockTable: Table,
    seconds: Int = 180)(testWith: TestWith[DynamoLockDao, R]): R = {
    val rowLockDaoConfig = DynamoLockDaoConfig(
      dynamoConfig = createDynamoConfigWith(lockTable),
      expiryTime = Duration.ofSeconds(seconds)
    )

    val dynamoLockDao = new DynamoLockDao(
      client = dynamoClient,
      config = rowLockDaoConfig
    )

    testWith(dynamoLockDao)
  }

  def withLockDao[R](dynamoDbClient: AmazonDynamoDB)(
    testWith: TestWith[DynamoLockDao, R]): R =
    withLocalDynamoDbTable { lockTable =>
      withLockDao(dynamoDbClient, lockTable) { lockDao =>
        testWith(lockDao)
      }
    }

  def withLockDao[R](lockTable: Table, seconds: Int)(
    testWith: TestWith[DynamoLockDao, R]): R =
    withLockDao(dynamoClient, lockTable = lockTable, seconds = seconds) {
      lockDao =>
        testWith(lockDao)
    }

  def createLockTable(table: Table): Table = {
    dynamoClient.createTable(
      new CreateTableRequest()
        .withTableName(table.name)
        .withKeySchema(new KeySchemaElement()
          .withAttributeName("id")
          .withKeyType(KeyType.HASH))
        .withAttributeDefinitions(
          new AttributeDefinition()
            .withAttributeName("id")
            .withAttributeType("S"),
          new AttributeDefinition()
            .withAttributeName("contextId")
            .withAttributeType("S")
        )
        .withProvisionedThroughput(new ProvisionedThroughput()
          .withReadCapacityUnits(1L)
          .withWriteCapacityUnits(1L))
        .withGlobalSecondaryIndexes(
          new GlobalSecondaryIndex()
            .withIndexName(table.index)
            .withProjection(
              new Projection().withProjectionType(ProjectionType.ALL))
            .withKeySchema(
              new KeySchemaElement()
                .withAttributeName("contextId")
                .withKeyType(KeyType.HASH)
            )
            .withProvisionedThroughput(new ProvisionedThroughput()
              .withReadCapacityUnits(1L)
              .withWriteCapacityUnits(1L))))
    eventually {
      waitUntilActive(dynamoClient, table.name)
    }
    table
  }
}
