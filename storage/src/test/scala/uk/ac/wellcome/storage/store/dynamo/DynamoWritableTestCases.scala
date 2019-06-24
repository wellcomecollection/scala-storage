package uk.ac.wellcome.storage.store.dynamo

import com.amazonaws.services.dynamodbv2.model.{AmazonDynamoDBException, ConditionalCheckFailedException, ResourceNotFoundException}
import org.scalatest.{Assertion, EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.storage.Version
import uk.ac.wellcome.storage.dynamo.DynamoEntry
import uk.ac.wellcome.storage.fixtures.DynamoFixtures
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table
import uk.ac.wellcome.storage.generators.{Record, RecordGenerators}

trait DynamoWritableTestCases[EntryType <: DynamoEntry[String, Record]]
  extends FunSpec
    with Matchers
    with DynamoFixtures
    with EitherValues
    with RecordGenerators {

  type HashKey = String

  type DynamoWritableStub = DynamoWritable[Version[HashKey, Int], EntryType, Record]

  def createDynamoWritableWith(table: Table, initialEntries: Set[EntryType] = Set.empty): DynamoWritableStub

  def createEntry(hashKey: String, v: Int, record: Record): EntryType

  def getRecord(table: Table)(hashKey: HashKey, v: Int): Record

  it("puts an entry in an empty table") {
    withLocalDynamoDbTable { table =>
      val writable = createDynamoWritableWith(table)

      val hashKey = randomAlphanumeric
      val record = createRecord

      writable.put(id = Version(hashKey, 1))(record) shouldBe a[Right[_, _]]

      getRecord(table)(hashKey, 1) shouldBe record
    }
  }

  describe("conditional puts") {
    val hashKey = randomAlphanumeric
    val olderRecord = createRecord
    val newerRecord = createRecord

    it("overwrites an old version with a new version") {
      withLocalDynamoDbTable { table =>
        val writable = createDynamoWritableWith(table, initialEntries = Set(
          createEntry(hashKey, 1, olderRecord)
        ))

        writable.put(id = Version(hashKey, 2))(newerRecord) shouldBe a[Right[_, _]]

        getRecord(table)(hashKey, 2) shouldBe newerRecord
      }
    }

    it("fails to overwrite the same version if it is already stored") {
      withLocalDynamoDbTable { table =>
        val writable = createDynamoWritableWith(table, initialEntries = Set(
          createEntry(hashKey, 2, newerRecord)
        ))

        val result = writable.put(id = Version(hashKey, 2))(newerRecord)

        val err = result.left.value
        err.e shouldBe a[ConditionalCheckFailedException]
        err.e.getMessage should startWith("The conditional request failed")
      }
    }
  }

  it("fails if DynamoDB fails") {
    val writable = createDynamoWritableWith(nonExistentTable)

    val hashKey = randomAlphanumeric
    val record = createRecord

    val result = writable.put(id = Version(hashKey, 1))(record)

    val err = result.left.value
    err.e shouldBe a[ResourceNotFoundException]
    err.e.getMessage should startWith("Cannot do operations on a non-existent table")
  }

  def assertErrorsOnWrongTableDefinition(createWrongTable: Table => Table, message: String): Assertion =
    withSpecifiedTable(createWrongTable) { table =>
      val writable = createDynamoWritableWith(table)

      val hashKey = randomAlphanumeric
      val record = createRecord

      val result = writable.put(id = Version(hashKey, 1))(record)

      val err = result.left.value
      err.e shouldBe a[AmazonDynamoDBException]
      err.e.getMessage should startWith(message)
    }

  def assertErrorsOnBadKeyName(createWrongTable: Table => Table): Assertion =
    assertErrorsOnWrongTableDefinition(createWrongTable, message = "One of the required keys was not given a value")

  def assertErrorsOnBadKeyType(createWrongTable: Table => Table): Assertion =
    assertErrorsOnWrongTableDefinition(createWrongTable, message = "Type mismatch for attribute to update")
}
