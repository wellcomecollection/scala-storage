package uk.ac.wellcome.storage.fixtures

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.gu.scanamo.syntax._
import com.gu.scanamo.{DynamoFormat, Scanamo}
import org.scalatest.Matchers
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import uk.ac.wellcome.storage.dynamo.{DynamoClientFactory, DynamoConfig, VersionedDao}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

object LocalDynamoDb {
  case class Table(name: String, index: String)
}

trait LocalDynamoDb extends Eventually with Matchers with IntegrationPatience {

  import LocalDynamoDb._

  private val port = 45678
  private val dynamoDBEndPoint = "http://localhost:" + port
  private val regionName = "localhost"
  private val accessKey = "access"
  private val secretKey = "secret"

  def dynamoDbLocalEndpointFlags(table: Table): Map[String, String] =
    dynamoClientLocalFlags ++ Map(
      "aws.dynamo.tableName" -> table.name,
      "aws.dynamo.tableIndex" -> table.index
    )

  def dynamoClientLocalFlags = Map(
    "aws.dynamoDb.endpoint" -> dynamoDBEndPoint,
    "aws.dynamoDb.accessKey" -> accessKey,
    "aws.dynamoDb.secretKey" -> secretKey,
    "aws.dynamoDb.region" -> regionName
  )

  val dynamoDbClient: AmazonDynamoDB = DynamoClientFactory.create(
    region = regionName,
    endpoint = dynamoDBEndPoint,
    accessKey = accessKey,
    secretKey = secretKey
  )

  def withSpecifiedLocalDynamoDbTable[R](
    createTable: (AmazonDynamoDB) => Table): Fixture[Table, R] =
    fixture[Table, R](
      create = createTable(dynamoDbClient),
      destroy = { table =>
        dynamoDbClient.deleteTable(table.name)
      }
    )

  def withLocalDynamoDbTable[R]: Fixture[Table, R] = fixture[Table, R](
    create = {
      val tableName = Random.alphanumeric.take(10).mkString
      val indexName = Random.alphanumeric.take(10).mkString

      createTable(Table(tableName, indexName))
    },
    destroy = { table =>
      dynamoDbClient.deleteTable(table.name)
    }
  )

  def withVersionedDao[R](table: Table)(
    testWith: TestWith[VersionedDao, R]): R = {
    val dynamoConfig = DynamoConfig(table = table.name, index = table.index)
    val dao = new VersionedDao(dynamoDbClient, dynamoConfig)
    testWith(dao)
  }

  def createTable(table: LocalDynamoDb.Table): Table

  def givenTableHasItem[T: DynamoFormat](item: T, table: Table) = {
    Scanamo.put(dynamoDbClient)(table.name)(item)
  }

  def getTableItem[T: DynamoFormat](id: String, table: Table) = {
    Scanamo.get[T](dynamoDbClient)(table.name)('id -> id)
  }

  def getExistingTableItem[T: DynamoFormat](id: String, table: Table) = {
    val record = Scanamo.get[T](dynamoDbClient)(table.name)('id -> id)
    record shouldBe 'defined
    record.get shouldBe 'right
    record.get.right.get
  }

  def assertTableEmpty[T: DynamoFormat](table: Table) = {
    val records = Scanamo.scan[T](dynamoDbClient)(table.name)
    records.size shouldBe 0
  }

  def assertTableHasItem[T: DynamoFormat](id: String, item: T, table: Table) = {
    val actualRecord = getTableItem(id, table)
    actualRecord shouldBe Some(Right(item))
  }

  def assertTableOnlyHasItem[T: DynamoFormat](item: T, table: Table) = {
    val records = Scanamo.scan[T](dynamoDbClient)(table.name)
    records.size shouldBe 1
    records.head shouldBe Right(item)
  }

  def listTableItems[T: DynamoFormat](table: Table) = {
    Scanamo.scan[T](dynamoDbClient)(table.name)
  }
}
