package uk.ac.wellcome.storage.fixtures

import java.time.Duration
import java.util.UUID

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.util.TableUtils.waitUntilActive
import org.scalatest.{Assertion, EitherValues, OptionValues}
import org.scanamo.auto._
import org.scanamo.time.JavaTimeFormats._
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table
import uk.ac.wellcome.storage.locking.dynamo.{DynamoLockDao, DynamoLockDaoConfig, DynamoLockingService, ExpiringLock}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Random

trait DynamoLockingFixtures extends DynamoFixtures with EitherValues with OptionValues {
  def assertNoLocks(lockTable: Table): Assertion =
    scanTable[ExpiringLock](lockTable) shouldBe empty

  def createRandomContextId: UUID = UUID.randomUUID()
  def createRandomId: String = Random.nextString(32)

  def withLockDao[R](
    dynamoDbClient: AmazonDynamoDB,
    lockTable: Table,
    seconds: Int = 180)(
    testWith: TestWith[DynamoLockDao, R]): R = {
    val rowLockDaoConfig = DynamoLockDaoConfig(
      dynamoConfig = createDynamoConfigWith(lockTable),
      expiryTime = Duration.ofSeconds(seconds)
    )

    val dynamoLockDao = new DynamoLockDao(
      client = dynamoDbClient,
      config = rowLockDaoConfig
    )

    testWith(dynamoLockDao)
  }

  def withLockDao[R](lockTable: Table)(
    testWith: TestWith[DynamoLockDao, R]): R =
    withLockDao(dynamoClient, lockTable = lockTable) { lockDao =>
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
    withLockDao(dynamoClient, lockTable = lockTable, seconds = seconds) { lockDao =>
      testWith(lockDao)
    }

  def withLockDao[R](
    testWith: TestWith[DynamoLockDao, R]): R =
    withLocalDynamoDbTable { lockTable =>
      withLockDao(dynamoClient, lockTable = lockTable) { lockDao =>
        testWith(lockDao)
      }
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

  type DynamoLockingServiceStub = DynamoLockingService[Unit, Future]

  def withDynamoLockingService[R](
    testWith: TestWith[DynamoLockingServiceStub, R])(implicit dynamoLockDao: DynamoLockDao): R = {
    val lockingService = new DynamoLockingService[Unit, Future]()
    testWith(lockingService)
  }

  def withDynamoLockingService[R](table: Table)(
    testWith: TestWith[DynamoLockingServiceStub, R]): R =
    withLockDao(table) { implicit lockDao =>
      withDynamoLockingService { lockingService =>
        testWith(lockingService)
      }
    }
}
