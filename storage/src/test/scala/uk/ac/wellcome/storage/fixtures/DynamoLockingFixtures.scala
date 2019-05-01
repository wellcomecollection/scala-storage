package uk.ac.wellcome.storage.fixtures

import java.time.Duration
import java.util.UUID

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.util.TableUtils.waitUntilActive
import com.gu.scanamo.Scanamo
import com.gu.scanamo.syntax._
import org.scalatest.{Assertion, EitherValues, OptionValues}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.dynamo._
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table
import uk.ac.wellcome.storage.locking.{DynamoLockDao, DynamoLockDaoConfig, DynamoLockingService, ExpiringLock}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

trait DynamoLockingFixtures extends LocalDynamoDb with EitherValues with OptionValues {
  def getDynamo(lockTable: Table)(id: String): ExpiringLock =
    Scanamo.get[ExpiringLock](
      dynamoDbClient
    )(
      lockTable.name
    )('id -> id).get.right.value

  def assertNoLocks(lockTable: Table): Assertion =
    Scanamo.scan[ExpiringLock](dynamoDbClient)(lockTable.name) shouldBe empty

  def createRandomContextId: UUID = UUID.randomUUID()
  def createRandomId: String = Random.nextString(32)

  def withLockDao[R](
    dynamoDbClient: AmazonDynamoDB,
    lockTable: Table,
    seconds: Int = 180)(
    testWith: TestWith[DynamoLockDao, R]): R = {
    val rowLockDaoConfig = DynamoLockDaoConfig(
      dynamoConfig = createDynamoConfigWith(lockTable),
      duration = Duration.ofSeconds(seconds)
    )

    val dynamoLockDao = new DynamoLockDao(
      client = dynamoDbClient,
      config = rowLockDaoConfig
    )

    testWith(dynamoLockDao)
  }

  def withLockDao[R](lockTable: Table)(
    testWith: TestWith[DynamoLockDao, R]): R =
    withLockDao(dynamoDbClient, lockTable = lockTable) { lockDao =>
      testWith(lockDao)
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
    withLockDao(dynamoDbClient, lockTable = lockTable, seconds = seconds) { lockDao =>
      testWith(lockDao)
    }

  def withLockDao[R](
    testWith: TestWith[DynamoLockDao, R]): R =
    withLocalDynamoDbTable { lockTable =>
      withLockDao(dynamoDbClient, lockTable = lockTable) { lockDao =>
        testWith(lockDao)
      }
    }

  def createLockTable(table: Table): Table = {
    dynamoDbClient.createTable(
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
      waitUntilActive(dynamoDbClient, table.name)
    }
    table
  }

  def withDynamoLockingService[R](dynamoLockDao: DynamoLockDao)(
    testWith: TestWith[DynamoLockingService, R]): R = {
    val lockingService = new DynamoLockingService()(dynamoLockDao)
    testWith(lockingService)
  }

  def withDynamoLockingService[R](table: Table)(
    testWith: TestWith[DynamoLockingService, R]): R =
    withLockDao(table) { lockDao =>
      withDynamoLockingService(lockDao) { lockingService =>
        testWith(lockingService)
      }
    }
}
