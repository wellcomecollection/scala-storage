package uk.ac.wellcome.storage.fixtures

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.util.TableUtils.waitUntilActive
import com.gu.scanamo.error.DynamoReadError
import com.gu.scanamo.syntax._
import com.gu.scanamo.{DynamoFormat, Scanamo}
import org.scalatest.Matchers
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import uk.ac.wellcome.fixtures._
import uk.ac.wellcome.storage.dynamo.{DynamoClientFactory, DynamoConfig}

import scala.util.Random

object DynamoFixtures {
  case class Table(name: String, index: String)
}

trait DynamoFixtures extends Eventually with Matchers with IntegrationPatience {
  import DynamoFixtures._

  private val port = 45678
  private val dynamoDBEndPoint = "http://localhost:" + port
  private val regionName = "localhost"
  private val accessKey = "access"
  private val secretKey = "secret"

  val dynamoClient: AmazonDynamoDB = DynamoClientFactory.create(
    region = regionName,
    endpoint = dynamoDBEndPoint,
    accessKey = accessKey,
    secretKey = secretKey
  )

  def withSpecifiedLocalDynamoDbTable[R](
    createTable: AmazonDynamoDB => Table): Fixture[Table, R] =
    fixture[Table, R](
      create = createTable(dynamoClient),
      destroy = { table =>
        dynamoClient.deleteTable(table.name)
      }
    )

  def withLocalDynamoDbTable[R]: Fixture[Table, R] = fixture[Table, R](
    create = {
      val tableName = Random.alphanumeric.take(10).mkString
      val indexName = Random.alphanumeric.take(10).mkString

      createTable(Table(tableName, indexName))
    },
    destroy = { table =>
      dynamoClient.deleteTable(table.name)
    }
  )

  def createTable(table: DynamoFixtures.Table): Table

  def givenTableHasItem[T: DynamoFormat](item: T, table: Table) = {
    Scanamo.put(dynamoClient)(table.name)(item)
  }

  def getTableItem[T: DynamoFormat](id: String, table: Table) = {
    Scanamo.get[T](dynamoClient)(table.name)('id -> id)
  }

  def getExistingTableItem[T: DynamoFormat](id: String, table: Table) = {
    val record = Scanamo.get[T](dynamoClient)(table.name)('id -> id)
    record shouldBe 'defined
    record.get shouldBe 'right
    record.get.right.get
  }

  def assertTableEmpty[T: DynamoFormat](table: Table) = {
    val records = Scanamo.scan[T](dynamoClient)(table.name)
    records.size shouldBe 0
  }

  def assertTableHasItem[T: DynamoFormat](id: String, item: T, table: Table) = {
    val actualRecord = getTableItem(id, table)
    actualRecord shouldBe Some(Right(item))
  }

  def assertTableOnlyHasItem[T: DynamoFormat](item: T, table: Table) = {
    val records = Scanamo.scan[T](dynamoClient)(table.name)
    records.size shouldBe 1
    records.head shouldBe Right(item)
  }

  def listTableItems[T: DynamoFormat](table: Table): List[Either[DynamoReadError, Any]] =
    Scanamo.scan[T](dynamoClient)(table.name)

  def createDynamoConfigWith(table: Table): DynamoConfig =
    DynamoConfig(
      tableName = table.name,
      maybeIndexName = Some(table.index)
    )

  def createTableFromRequest(table: Table, request: CreateTableRequest): Table = {
    dynamoClient.createTable(request)

    eventually {
      waitUntilActive(dynamoClient, table.name)
    }
    table
  }

  def createTableWithHashKey(
    table: Table,
    keyName: String,
    keyType: ScalarAttributeType = ScalarAttributeType.S
  ): Table =
    createTableFromRequest(
      table = table,
      new CreateTableRequest()
        .withTableName(table.name)
        .withKeySchema(new KeySchemaElement()
          .withAttributeName(keyName)
          .withKeyType(KeyType.HASH))
        .withAttributeDefinitions(
          new AttributeDefinition()
            .withAttributeName(keyName)
            .withAttributeType(keyType)
        )
        .withProvisionedThroughput(new ProvisionedThroughput()
          .withReadCapacityUnits(1L)
          .withWriteCapacityUnits(1L))
    )

  def createTableWithHashRangeKey(
    table: Table,
    hashKeyName: String,
    hashKeyType: ScalarAttributeType = ScalarAttributeType.S,
    rangeKeyName: String,
    rangeKeyType: ScalarAttributeType = ScalarAttributeType.S): Table =
    createTableFromRequest(
      table = table,
      new CreateTableRequest()
        .withTableName(table.name)
        .withKeySchema(new KeySchemaElement()
          .withAttributeName(hashKeyName)
          .withKeyType(KeyType.HASH))
        .withKeySchema(new KeySchemaElement()
          .withAttributeName(rangeKeyName)
          .withKeyType(KeyType.RANGE))
        .withAttributeDefinitions(
          new AttributeDefinition()
            .withAttributeName(hashKeyName)
            .withAttributeType(hashKeyType),
          new AttributeDefinition()
            .withAttributeName(rangeKeyName)
            .withAttributeType(rangeKeyType)
        )
        .withProvisionedThroughput(new ProvisionedThroughput()
          .withReadCapacityUnits(1L)
          .withWriteCapacityUnits(1L))
    )
}
