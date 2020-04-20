package uk.ac.wellcome.storage.store.dynamo

import com.amazonaws.services.dynamodbv2.model.{AmazonDynamoDBException, ResourceNotFoundException}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, EitherValues}
import org.scanamo.{Table => ScanamoTable}
import uk.ac.wellcome.storage.dynamo.DynamoEntry
import uk.ac.wellcome.storage.fixtures.DynamoFixtures
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table
import uk.ac.wellcome.storage.generators.{Record, RecordGenerators}
import uk.ac.wellcome.storage.{DoesNotExistError, Identified, Version}
import org.scanamo.auto._

trait DynamoReadableTestCases[
  DynamoIdent, EntryType <: DynamoEntry[String, Record]]
    extends AnyFunSpec
    with Matchers
    with DynamoFixtures
    with EitherValues
    with RecordGenerators {

  type HashKey = String

  type DynamoReadableStub =
    DynamoReadable[Version[HashKey, Int], DynamoIdent, EntryType, Record]

  // TODO: Make initialEntries an arbitrary type
  def createDynamoReadableWith(
    table: Table,
    initialEntries: Set[EntryType] = Set.empty): DynamoReadableStub

  def createEntry(hashKey: String, v: Int, record: Record): EntryType

  describe("DynamoReadable") {
    it("reads a row from the table") {
      val id = randomAlphanumeric
      val record = createRecord

      val initialEntries = Set(
        createEntry(id, v = 1, record)
      )

      withLocalDynamoDbTable { table =>
        val readable = createDynamoReadableWith(table, initialEntries)

        readable.get(Version(id, 1)).right.value shouldBe Identified(
          Version(id, 1),
          record)
      }
    }

    it("finds nothing if the table is empty") {
      withLocalDynamoDbTable { table =>
        val readable = createDynamoReadableWith(table)

        readable
          .get(Version(randomAlphanumeric, 1))
          .left
          .value shouldBe a[DoesNotExistError]
      }
    }

    it("finds nothing if there's no row with that hash key") {
      val id = randomAlphanumeric

      val initialEntries = Set(
        createEntry(id, v = 1, createRecord)
      )

      withLocalDynamoDbTable { table =>
        val readable = createDynamoReadableWith(table, initialEntries)

        readable
          .get(Version(randomAlphanumeric, 1))
          .left
          .value shouldBe a[DoesNotExistError]
      }
    }

    it("fails if DynamoDB has an error") {
      val readable = createDynamoReadableWith(nonExistentTable)

      val result = readable.get(Version(randomAlphanumeric, 1))
      val err = result.left.value.e

      err shouldBe a[ResourceNotFoundException]
      err.getMessage should startWith(
        "Cannot do operations on a non-existent table")
    }

    it("fails if the row doesn't match the model") {
      // This doesn't have the payload field that our DynamoEntry model requires
      case class BadModel(id: String, version: Int, t: String)

      val id = randomAlphanumeric

      withLocalDynamoDbTable { table =>
        scanamo.exec(
          ScanamoTable[BadModel](table.name).putAll(
            Set(BadModel(id, version = 1, t = randomAlphanumeric))
          ))

        val readable = createDynamoReadableWith(table)

        val result = readable.get(Version(id, 1))
        val err = result.left.value.e

        err shouldBe a[Error]
        err.getMessage should startWith(
          "DynamoReadError: InvalidPropertiesError")
      }
    }
  }

  def assertErrorsOnWrongTableDefinition(createWrongTable: Table => Table,
                                         message: String): Assertion =
    withSpecifiedTable(createWrongTable) { table =>
      val readable = createDynamoReadableWith(table)

      val result = readable.get(id = Version(randomAlphanumeric, 1))

      val err = result.left.value
      err.e shouldBe a[AmazonDynamoDBException]
      err.e.getMessage should startWith(message)
    }

  def assertErrorsOnBadKeyName(createWrongTable: Table => Table): Assertion =
    assertErrorsOnWrongTableDefinition(
      createWrongTable,
      message = "Query condition missed key schema element")

  def assertErrorsOnBadKeyType(createWrongTable: Table => Table): Assertion =
    assertErrorsOnWrongTableDefinition(
      createWrongTable,
      message =
        "One or more parameter values were invalid: Condition parameter type does not match schema type")
}
