package uk.ac.wellcome.storage.dynamo

import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.util.TableUtils.waitUntilActive
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table

import scala.concurrent.ExecutionContext.Implicits.global

class DynamoHashKeyLookupTest extends FunSpec with Matchers with ScalaFutures with LocalDynamoDb {

  case class Payload(
    id: String,
    version: Int,
    contents: String
  )

  def createTable(table: Table): Table = {
    dynamoDbClient.createTable(
      new CreateTableRequest()
        .withTableName(table.name)
        .withKeySchema(
          new KeySchemaElement()
            .withAttributeName("id")
            .withKeyType(KeyType.HASH)
        )
        .withKeySchema(
          new KeySchemaElement()
            .withAttributeName("version")
            .withKeyType(KeyType.RANGE)
        )
        .withAttributeDefinitions(
          new AttributeDefinition()
            .withAttributeName("id")
            .withAttributeType("S"),
          new AttributeDefinition()
            .withAttributeName("version")
            .withAttributeType("N")
        )
        .withProvisionedThroughput(new ProvisionedThroughput()
          .withReadCapacityUnits(1L)
          .withWriteCapacityUnits(1L))
    )
    eventually {
      waitUntilActive(dynamoDbClient, table.name)
    }
    table
  }

  def withDynamoLookup[R](table: Table)(testWith: TestWith[DynamoHashKeyLookup[Payload], R]): R = {
    val lookup = new DynamoHashKeyLookup[Payload](
      dynamoClient = dynamoDbClient,
      dynamoConfig = createDynamoConfigWith(table)
    )

    testWith(lookup)
  }

  def withDynamoLookup[R](testWith: TestWith[DynamoHashKeyLookup[Payload], R]): R =
    withLocalDynamoDbTable { table =>
      withDynamoLookup(table) { testWith }
    }

  it("returns None if the table is empty") {
    withDynamoLookup { lookup =>
      whenReady(lookup.lookupHighestHashKey("id", value = "123")) { _ shouldBe None }
      whenReady(lookup.lookupLowestHashKey("id", value = "123")) { _ shouldBe None }
    }
  }
}
