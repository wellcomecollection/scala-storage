package uk.ac.wellcome.storage.dynamo

import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.util.TableUtils.waitUntilActive
import com.gu.scanamo.Scanamo
import org.scalatest.{FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table

import scala.util.{Failure, Success}

class DynamoHashKeyLookupTest extends FunSpec with Matchers with LocalDynamoDb {

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

  def withDynamoLookup[R](table: Table, hashKeyName: String = "id")(testWith: TestWith[DynamoHashKeyLookupStub, R]): R = {
    val lookup = new DynamoHashKeyLookup[Payload, String](
      dynamoClient = dynamoDbClient,
      lookupConfig = DynamoHashKeyLookupConfig(
        hashKeyName = hashKeyName,
        dynamoConfig = createDynamoConfigWith(table)
      )
    )

    testWith(lookup)
  }

  def withDynamoLookup[R](testWith: TestWith[DynamoHashKeyLookupStub, R]): R =
    withLocalDynamoDbTable { table =>
      withDynamoLookup(table) { testWith }
    }

  it("returns None if it can't find any rows with the given hash key value") {
    withDynamoLookup { lookup =>
      lookup.lookupHighestHashKey(value = "123") shouldBe Success(None)
      lookup.lookupLowestHashKey(value = "123") shouldBe Success(None)
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
        val resultHigh = lookup.lookupHighestHashKey(value = "123")

        resultHigh shouldBe a[Success[_]]
        val payloadHigh = resultHigh.get.get
        payloadHigh.id shouldBe "123"
        payloadHigh.version shouldBe 3
        payloadHigh.contents shouldBe "Payload the third"

        val resultLow = lookup.lookupLowestHashKey(value = "123")

        resultLow shouldBe a[Success[_]]
        val payloadLow = resultLow.get.get
        payloadLow.id shouldBe "123"
        payloadLow.version shouldBe 1
        payloadLow.contents shouldBe "Payload the first"
      }
    }
  }

  it("finds the same value if there's only a single instance of a hash key") {
    withLocalDynamoDbTable { table =>
      put(table, Payload(id = "123", version = 1, contents = "Payload the first"))

      withDynamoLookup(table) { lookup =>
        val resultHigh = lookup.lookupHighestHashKey(value = "123")

        resultHigh shouldBe a[Success[_]]
        val payloadHigh = resultHigh.get.get
        payloadHigh.id shouldBe "123"
        payloadHigh.version shouldBe 1
        payloadHigh.contents shouldBe "Payload the first"


        val resultLow = lookup.lookupLowestHashKey(value = "123")

        resultLow shouldBe a[Success[_]]
        val payloadLow = resultLow.get.get
        payloadLow.id shouldBe "123"
        payloadLow.version shouldBe 1
        payloadLow.contents shouldBe "Payload the first"
      }
    }
  }

  it("returns a failure if you pass the wrong hash key type") {
    withLocalDynamoDbTable { table =>
      val brokenLookup = new DynamoHashKeyLookup[Payload, Int](
        dynamoClient = dynamoDbClient,
        lookupConfig = DynamoHashKeyLookupConfig(
          hashKeyName = "id",
          dynamoConfig = createDynamoConfigWith(table)
        )
      )

      val result = brokenLookup.lookupHighestHashKey(value = 123)

      result shouldBe a[Failure[_]]
      val err = result.failed.get
      err shouldBe a[AmazonDynamoDBException]
      err.getMessage should startWith("One or more parameter values were invalid")
    }
  }

  it("returns a failure if you ask for a different case class") {
    withLocalDynamoDbTable { table =>
      put(table, Payload(id = "123", version = 1, contents = "Payload the first"))
      put(table, Payload(id = "123", version = 2, contents = "Payload the second"))
      put(table, Payload(id = "123", version = 3, contents = "Payload the third"))

      case class DifferentPayload(
        id: Int,
        version: String,
        text: String
      )

      val brokenLookup = new DynamoHashKeyLookup[DifferentPayload, String](
        dynamoClient = dynamoDbClient,
        lookupConfig = DynamoHashKeyLookupConfig(
          hashKeyName = "id",
          dynamoConfig = createDynamoConfigWith(table)
        )
      )

      val result = brokenLookup.lookupHighestHashKey(value = "123")

      result shouldBe a[Failure[_]]
      val err = result.failed.get
      err shouldBe a[RuntimeException]
      err.getMessage should startWith("Error parsing")
    }
  }

  it("returns a failure if you pass the wrong hash key name") {
    withLocalDynamoDbTable { table =>
      withDynamoLookup(table, hashKeyName = "identifier") { lookup =>
        val result = lookup.lookupHighestHashKey(value = "123")

        result shouldBe a[Failure[_]]
        val err = result.failed.get
        err shouldBe a[AmazonDynamoDBException]
        err.getMessage should startWith("Query condition missed key schema element")
      }
    }
  }

  it("returns a failure if it can't find the table") {
    val doesNotExist = Table("does-not-exist", "no-such-index")

    withDynamoLookup(doesNotExist) { brokenLookup =>
      val result = brokenLookup.lookupHighestHashKey(value = "123")

      result shouldBe a[Failure[_]]
      val err = result.failed.get
      err shouldBe a[AmazonDynamoDBException]
      err.getMessage should startWith("Cannot do operations on a non-existent table")
    }
  }

  private def put(table: Table, p: Payload) =
    Scanamo.put[Payload](dynamoDbClient)(table.name)(p)
}
