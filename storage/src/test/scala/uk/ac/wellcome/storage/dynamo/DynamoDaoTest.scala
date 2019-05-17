package uk.ac.wellcome.storage.dynamo

import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.util.TableUtils.waitUntilActive
import org.scalatest.{Assertion, FunSpec, Matchers}
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table

import scala.util.{Failure, Success}

class DynamoDaoTest extends FunSpec with Matchers with LocalDynamoDb {
  override def createTable(table: LocalDynamoDb.Table): LocalDynamoDb.Table = {
    dynamoDbClient.createTable(
      new CreateTableRequest()
        .withTableName(table.name)
        .withKeySchema(new KeySchemaElement()
          .withAttributeName("id")
          .withKeyType(KeyType.HASH))
        .withAttributeDefinitions(
          new AttributeDefinition()
            .withAttributeName("id")
            .withAttributeType("S")
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

  case class Record(
    id: String,
    data: String
  )

  it("behaves as a dao") {
    withLocalDynamoDbTable { table =>
      withDynamoDao[Record, Assertion](table) { dao =>
        dao.get(id = "1") shouldBe Success(None)
        dao.get(id = "2") shouldBe Success(None)

        val r1 = Record(id = "1", data = "hello world")
        dao.put(r1) shouldBe Success(r1)
        dao.get(id = "1") shouldBe Success(Some(r1))

        val r2 = Record(id = "2", data = "howdy friends")
        dao.put(r2) shouldBe Success(r2)
        dao.get(id = "2") shouldBe Success(Some(r2))

        val r1Update = r1.copy(data = "what's up, folks?")
        dao.put(r1Update) shouldBe Success(r1Update)
        dao.get(id = "1") shouldBe Success(Some(r1Update))
      }
    }
  }

  it("stores records in DynamoDB") {
    withLocalDynamoDbTable { table =>
      withDynamoDao[Record, Assertion](table) { dao =>
        val r = Record(id = "1", data = "hello world")
        dao.put(r)

        assertTableHasItem(id = "1", r, table)
      }
    }
  }

  it("fails if it can't get from DynamoDB") {
    withDynamoDao[Record, Assertion](Table("does-not-exist", "does-not-exist")) { dao =>
      val result = dao.get(id = "1")

      result shouldBe a[Failure[_]]
      val err = result.failed.get
      err shouldBe a[AmazonDynamoDBException]
      err.getMessage should startWith("Cannot do operations on a non-existent table")
    }
  }

  it("fails if it can't put to DynamoDB") {
    withDynamoDao[Record, Assertion](Table("does-not-exist", "does-not-exist")) { dao =>
      val r = Record(id = "1", data = "hello world")
      val result = dao.put(r)

      result shouldBe a[Failure[_]]
      val err = result.failed.get
      err shouldBe a[AmazonDynamoDBException]
      err.getMessage should startWith("Cannot do operations on a non-existent table")
    }
  }

  it("fails if the DynamoDB data is in the wrong format") {
    case class NameRecord(
      id: String,
      name: String
    )

    withLocalDynamoDbTable { table =>
      givenTableHasItem(item = NameRecord(id = "1", name = "henry"), table)

      withDynamoDao[Record, Assertion](table) { dao =>
        val result = dao.get(id = "1")

        result shouldBe a[Failure[_]]
        val err = result.failed.get
        err shouldBe a[RuntimeException]
        err.getMessage should startWith("InvalidPropertiesError")
      }
    }
  }
}
