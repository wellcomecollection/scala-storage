package uk.ac.wellcome.storage.fixtures

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.util.TableUtils.waitUntilActive
import org.scalatest.Matchers
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scanamo.error.DynamoReadError
import org.scanamo.syntax._
import org.scanamo.{DynamoFormat, Scanamo, Table => ScanamoTable}
import uk.ac.wellcome.fixtures._
import uk.ac.wellcome.storage.dynamo.{DynamoClientFactory, DynamoConfig}

import scala.collection.immutable
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

  val scanamo = Scanamo(dynamoClient)

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

  def putTableItem[T: DynamoFormat](item: T, table: Table): Option[Either[DynamoReadError, T]] =
    scanamo.exec(
      ScanamoTable[T](table.name).put(item)
    )

  def getTableItem[T: DynamoFormat](id: String, table: Table): Option[Either[DynamoReadError, T]] =
    scanamo.exec(
      ScanamoTable[T](table.name).get('id -> id)
    )

  def getExistingTableItem[T: DynamoFormat](id: String, table: Table): T = {
    val record = getTableItem[T](id, table)
    record shouldBe 'defined
    record.get shouldBe 'right
    record.get.right.get
  }

  def scanTable[T: DynamoFormat](table: Table): immutable.Seq[Either[DynamoReadError, Any]] =
    scanamo.exec(
      ScanamoTable[T](table.name).scan()
    )

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
