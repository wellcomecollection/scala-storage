package uk.ac.wellcome.storage.fixtures

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.gu.scanamo.error.DynamoReadError
import com.gu.scanamo.syntax._
import com.gu.scanamo.{DynamoFormat, Scanamo}
import org.scalatest.Matchers
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import uk.ac.wellcome.fixtures._
import uk.ac.wellcome.storage.dynamo.{DynamoClientFactory, DynamoConditionalUpdateDao, DynamoConfig, DynamoDao, DynamoVersionedDao, UpdateExpressionGenerator}
import uk.ac.wellcome.storage.type_classes.{IdGetter, VersionGetter, VersionUpdater}

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

  def withVersionedDao[T, R](table: Table)(
    testWith: TestWith[DynamoVersionedDao[T], R])(
    implicit
    evidence: DynamoFormat[T],
    versionUpdater: VersionUpdater[T],
    idGetter: IdGetter[T],
    versionGetter: VersionGetter[T],
    updateExpressionGenerator: UpdateExpressionGenerator[T]): R =
    withDynamoConditionalUpdateDao[T, R](table) { conditionalUpdateDao =>
      val dao = new DynamoVersionedDao[T](conditionalUpdateDao)

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

  def listTableItems[T: DynamoFormat](table: Table): List[Either[DynamoReadError, Any]] =
    Scanamo.scan[T](dynamoDbClient)(table.name)

  def createDynamoConfigWith(table: Table): DynamoConfig =
    DynamoConfig(
      table = table.name,
      maybeIndex = Some(table.index)
    )

  def withDynamoDao[T, R](table: Table)(testWith: TestWith[DynamoDao[T], R])(
    implicit
    evidence: DynamoFormat[T],
    idGetter: IdGetter[T],
    updateExpressionGenerator: UpdateExpressionGenerator[T]): R = {
    val dao = new DynamoDao[T](
      dynamoClient = dynamoDbClient,
      dynamoConfig = createDynamoConfigWith(table)
    )

    testWith(dao)
  }

  def withDynamoConditionalUpdateDao[T, R](table: Table)(testWith: TestWith[DynamoConditionalUpdateDao[T], R])(
    implicit
    evidence: DynamoFormat[T],
    idGetter: IdGetter[T],
    versionGetter: VersionGetter[T],
    updateExpressionGenerator: UpdateExpressionGenerator[T]): R =
    withDynamoDao[T, R](table) { underlying =>
      val dao = new DynamoConditionalUpdateDao[T](underlying)

      testWith(dao)
    }
}
