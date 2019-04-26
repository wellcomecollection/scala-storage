package uk.ac.wellcome.storage.fixtures

import java.time.{Duration, Instant}

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.util.TableUtils.waitUntilActive
import com.gu.scanamo.DynamoFormat
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.monitoring.MetricsSender
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table
import uk.ac.wellcome.storage.locking.{DynamoLockingService, DynamoLockDao, DynamoRowLockDaoConfig}

import scala.concurrent.ExecutionContext.Implicits.global

trait LockingFixtures extends LocalDynamoDb {
  implicit val instantLongFormat: AnyRef with DynamoFormat[Instant] =
    DynamoFormat.coercedXmap[Instant, Long, IllegalArgumentException](
      Instant.ofEpochSecond
    )(
      _.getEpochSecond
    )

  def withDynamoRowLockDao[R](dynamoDbClient: AmazonDynamoDB, lockTable: Table)(
    testWith: TestWith[DynamoLockDao, R]): R = {
    val rowLockDaoConfig = DynamoRowLockDaoConfig(
      dynamoConfig = createDynamoConfigWith(lockTable),
      duration = Duration.ofSeconds(180)
    )

    val dynamoRowLockDao = new DynamoLockDao(
      dynamoDbClient = dynamoDbClient,
      rowLockDaoConfig = rowLockDaoConfig
    )

    testWith(dynamoRowLockDao)
  }

  def withDynamoRowLockDao[R](lockTable: Table)(
    testWith: TestWith[DynamoLockDao, R]): R =
    withDynamoRowLockDao(dynamoDbClient, lockTable = lockTable) { rowLockDao =>
      testWith(rowLockDao)
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

  val lockNamePrefix = "locking.test"

  def withLockingService[R](dynamoRowLockDao: DynamoLockDao,
                            metricsSender: MetricsSender)(
                             testWith: TestWith[DynamoLockingService, R]): R = {
    val lockingService = new DynamoLockingService(
      lockNamePrefix = lockNamePrefix,
      dynamoRowLockDao = dynamoRowLockDao,
      metricsSender = metricsSender
    )
    testWith(lockingService)
  }
}
