package uk.ac.wellcome.storage.dynamo

import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.util.TableUtils.waitUntilActive
import com.gu.scanamo.Scanamo
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

  type DynamoHashKeyLookupStub = DynamoHashKeyLookup[Payload, String]

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

  def withDynamoLookup[R](table: Table)(testWith: TestWith[DynamoHashKeyLookupStub, R]): R = {
    val lookup = new DynamoHashKeyLookup[Payload, String](
      dynamoClient = dynamoDbClient,
      dynamoConfig = createDynamoConfigWith(table)
    )

    testWith(lookup)
  }

  def withDynamoLookup[R](testWith: TestWith[DynamoHashKeyLookupStub, R]): R =
    withLocalDynamoDbTable { table =>
      withDynamoLookup(table) { testWith }
    }

  it("returns None if it can't find any rows with the given hash key value") {
    withDynamoLookup { lookup =>
      whenReady(lookup.lookupHighestHashKey("id", value = "123")) { _ shouldBe None }
      whenReady(lookup.lookupLowestHashKey("id", value = "123")) { _ shouldBe None }
    }
  }

  it("finds the highest/lowest values of a hash key") {
    withLocalDynamoDbTable { table =>
      put(table, Payload(id = "123", version = 1, contents = "Payload the first"))
      put(table, Payload(id = "123", version = 2, contents = "Payload the second"))
      put(table, Payload(id = "123", version = 3, contents = "Payload the third"))

      put(table, Payload(id = "456", version = 1, contents = "A different payload"))
      put(table, Payload(id = "456", version = 2, contents = "Another version of a different payload"))

      withDynamoLookup(table) { lookup =>
        whenReady(lookup.lookupHighestHashKey("id", value = "123")) { result =>
          val payload = result.get
          payload.id shouldBe "123"
          payload.version shouldBe 3
          payload.contents shouldBe "Payload the third"
        }

        whenReady(lookup.lookupLowestHashKey("id", value = "123")) { result =>
          val payload = result.get
          payload.id shouldBe "123"
          payload.version shouldBe 1
          payload.contents shouldBe "Payload the first"
        }
      }
    }
  }

  it("finds the same value if there's only a single instance of a hash key") {
    withLocalDynamoDbTable { table =>
      put(table, Payload(id = "123", version = 1, contents = "Payload the first"))

      withDynamoLookup(table) { lookup =>
        whenReady(lookup.lookupHighestHashKey("id", value = "123")) { result =>
          val payload = result.get
          payload.id shouldBe "123"
          payload.version shouldBe 1
          payload.contents shouldBe "Payload the first"
        }

        whenReady(lookup.lookupLowestHashKey("id", value = "123")) { result =>
          val payload = result.get
          payload.id shouldBe "123"
          payload.version shouldBe 1
          payload.contents shouldBe "Payload the first"
        }
      }
    }
  }

  it("returns a failure if you pass the wrong hash key type") {
    withLocalDynamoDbTable { table =>
      val brokenLookup = new DynamoHashKeyLookup[Payload, Int](
        dynamoClient = dynamoDbClient,
        dynamoConfig = createDynamoConfigWith(table)
      )

      whenReady(brokenLookup.lookupHighestHashKey("id", value = 123).failed) { err =>
        err shouldBe a[AmazonDynamoDBException]
        err.getMessage should startWith("One or more parameter values were invalid")
      }
    }
  }

  it("returns a failure if it can't find the table") {
    val brokenLookup = new DynamoHashKeyLookup[Payload, String](
      dynamoClient = dynamoDbClient,
      dynamoConfig = createDynamoConfigWith(Table("does-not-exist", "no-such-index"))
    )

    whenReady(brokenLookup.lookupHighestHashKey("id", value = "123").failed) { err =>
      err shouldBe a[AmazonDynamoDBException]
      err.getMessage should startWith("Cannot do operations on a non-existent table")
    }
  }

  private def put(table: Table, p: Payload) =
    Scanamo.put[Payload](dynamoDbClient)(table.name)(p)
}
