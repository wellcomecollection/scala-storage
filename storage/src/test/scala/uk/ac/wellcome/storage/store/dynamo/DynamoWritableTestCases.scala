package uk.ac.wellcome.storage.store.dynamo

import com.amazonaws.services.dynamodbv2.model.{AmazonDynamoDBException, ConditionalCheckFailedException, ResourceNotFoundException}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, EitherValues}
import uk.ac.wellcome.storage.{RetryableError, Version}
import uk.ac.wellcome.storage.dynamo.DynamoEntry
import uk.ac.wellcome.storage.fixtures.DynamoFixtures
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table

trait DynamoWritableTestCases[Ident, T, EntryType <: DynamoEntry[Ident, T]]
    extends AnyFunSpec
    with Matchers
    with DynamoFixtures
    with EitherValues {

  def createId: Ident
  def createT: T

  type DynamoWritableStub = DynamoWritable[Version[Ident, Int], EntryType, T]

  def createDynamoWritableWith(
    table: Table,
    initialEntries: Set[EntryType] = Set.empty): DynamoWritableStub

  def createEntry(hashKey: Ident, v: Int, t: T): EntryType

  def getT(table: Table)(hashKey: Ident, v: Int): T

  describe("DynamoWritable") {
    it("puts an entry in an empty table") {
      withLocalDynamoDbTable { table =>
        val writable = createDynamoWritableWith(table)

        val hashKey = createId
        val t = createT

        writable.put(id = Version(hashKey, 1))(t) shouldBe a[Right[_, _]]

        getT(table)(hashKey, 1) shouldBe t
      }
    }

    describe("conditional puts") {
      val hashKey = createId
      val olderT = createT
      val newerT = createT

      it("overwrites an old version with a new version") {
        withLocalDynamoDbTable { table =>
          val writable = createDynamoWritableWith(
            table,
            initialEntries = Set(
              createEntry(hashKey, 1, olderT)
            ))

          writable.put(id = Version(hashKey, 2))(newerT) shouldBe a[Right[_,
                                                                          _]]

          getT(table)(hashKey, 2) shouldBe newerT
        }
      }

      it("fails to overwrite the same version if it is already stored") {
        withLocalDynamoDbTable { table =>
          val writable = createDynamoWritableWith(
            table,
            initialEntries = Set(
              createEntry(hashKey, 2, newerT)
            ))

          val result = writable.put(id = Version(hashKey, 2))(newerT)

          val err = result.left.value
          err shouldBe a[RetryableError]
          err.e shouldBe a[ConditionalCheckFailedException]
          err.e.getMessage should startWith("The conditional request failed")
        }
      }
    }

    it("fails if DynamoDB fails") {
      val writable = createDynamoWritableWith(nonExistentTable)

      val result = writable.put(id = Version(createId, 1))(createT)

      val err = result.left.value
      err.e shouldBe a[ResourceNotFoundException]
      err.e.getMessage should startWith(
        "Cannot do operations on a non-existent table")
    }
  }

  def assertErrorsOnWrongTableDefinition(createWrongTable: Table => Table,
                                         message: String): Assertion =
    withSpecifiedTable(createWrongTable) { table =>
      val writable = createDynamoWritableWith(table)

      val result = writable.put(id = Version(createId, 1))(createT)

      val err = result.left.value
      err.e shouldBe a[AmazonDynamoDBException]
      err.e.getMessage should startWith(message)
    }

  def assertErrorsOnBadKeyName(createWrongTable: Table => Table): Assertion =
    assertErrorsOnWrongTableDefinition(
      createWrongTable,
      message = "One of the required keys was not given a value")

  def assertErrorsOnBadKeyType(createWrongTable: Table => Table): Assertion =
    assertErrorsOnWrongTableDefinition(
      createWrongTable,
      message = "Type mismatch for attribute to update")
}
