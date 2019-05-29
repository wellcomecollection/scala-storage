package uk.ac.wellcome.storage.dynamo

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.{ConditionalCheckFailedException, GetItemRequest, ProvisionedThroughputExceededException, UpdateItemRequest}
import com.gu.scanamo.Scanamo
import org.mockito.Matchers.any
import org.mockito.Mockito.when
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Assertion, EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb
import uk.ac.wellcome.storage.fixtures.LocalDynamoDb.Table
import uk.ac.wellcome.storage.{DaoReadError, DoesNotExistError}

case class Record(
  id: String,
  data: String,
  version: Int
)

case class ExtendedRecord(
  id: String,
  data: String,
  version: Int,
  moreData: Int
)

class DynamoVersionedDaoTest
    extends FunSpec
    with LocalDynamoDb
    with MockitoSugar
    with Matchers
    with EitherValues {

  def createTable(table: Table): Table =
    createTableWithHashKey(table, keyName = "id")

  def withFixtures[R](testWith: TestWith[(Table, DynamoVersionedDao[String, Record]), R]): R = {
    withLocalDynamoDbTable[R] { table =>
      withVersionedDao[Record, R](table) { versionedDao =>
        testWith((table, versionedDao))
      }
    }
  }

  describe("get a record") {
    it("returns the record if it's in Dynamo") {
      withFixtures {
        case (table, versionedDao) =>
          val record = Record(
            id = "testSource/b110101001",
            data = "whatever",
            version = 0
          )

          Scanamo.put(dynamoDbClient)(table.name)(record)

          val result = versionedDao.get(record.id)
          result shouldBe Right(record)
      }
    }

    it("returns None if the record isn't in Dynamo") {
      withFixtures { case (_, versionedDao) =>
        val result = versionedDao.get("testSource/b88888")
        result.left.value shouldBe a[DoesNotExistError]
      }
    }

    it("fails if reading from DynamoDB fails") {
      withLocalDynamoDbTable { table =>
        val mockDynamoDbClient = mock[AmazonDynamoDB]
        val expectedException = new RuntimeException("AAAAAARGH!")
        when(mockDynamoDbClient.getItem(any[GetItemRequest]))
          .thenThrow(expectedException)

        val failingDao = createDaoWith(mockDynamoDbClient, table)

        val result = failingDao.get("testSource/b88888")
        result.left.value shouldBe DaoReadError(expectedException)
      }
    }
  }

  describe("update a record") {
    it("inserts a new record if it doesn't already exist") {
      withFixtures {
        case (table, versionedDao) =>
          val newRecord = Record(
            id = "testSource/b1111",
            data = "whatever",
            version = 0
          )

          val expectedRecord = newRecord.copy(version = 1)

          versionedDao.put(newRecord)

          assertTableHasItem(id = newRecord.id, expectedRecord, table)
      }
    }

    it("updates an existing record if the update has a higher version") {
      withFixtures {
        case (table, versionedDao) =>
          val existingRecord = Record(
            id = "testSource/b1111",
            data = "whatever",
            version = 0
          )

          val updatedRecord = existingRecord.copy(version = 2)

          Scanamo.put(dynamoDbClient)(table.name)(existingRecord)

          versionedDao.put(updatedRecord)

          val expectedTestVersioned = updatedRecord.copy(version = 3)
          assertTableHasItem(id = existingRecord.id, expectedTestVersioned, table)
      }
    }

    it("updates a record if it already exists and has the same version") {
      withFixtures {
        case (table, versionedDao) =>
          val existingRecord = Record(
            id = "testSource/b1111",
            data = "whatever",
            version = 1
          )

          Scanamo.put(dynamoDbClient)(table.name)(existingRecord)

          versionedDao.put(existingRecord)

          val expectedRecord = existingRecord.copy(version = 2)
          assertTableHasItem(id = existingRecord.id, expectedRecord, table)
      }
    }

    it("does not update an existing record if the update has a lower version") {
      withFixtures {
        case (table, versionedDao) =>
          val updateRecord = Record(
            id = "testSource/b1111",
            data = "whatever",
            version = 1
          )

          val existingRecord = Record(
            id = "testSource/b1111",
            data = "whatever",
            version = 2
          )

          Scanamo.put(dynamoDbClient)(table.name)(existingRecord)

          val result = versionedDao.put(updateRecord)
          val err = result.left.value.e
          err shouldBe a[ConditionalCheckFailedException]
          err.getMessage should startWith("The conditional request failed")

          assertTableHasItem(id = updateRecord.id, existingRecord, table)
      }
    }

    it(
      "does not remove fields from a record if updating only a subset of fields in a record") {
      withFixtures {
        case (table, versionedDao) =>
          val id = "111"
          val version = 3

          val extendedRecord = ExtendedRecord(
            id = id,
            data = "A friendly fish fry with francis and frankie in France.",
            version = version,
            moreData = 0
          )

          Scanamo.put(dynamoDbClient)(table.name)(extendedRecord)

          val result = versionedDao.get(extendedRecord.id)
          result shouldBe Right(
            Record(
              id = extendedRecord.id,
              data = extendedRecord.data,
              version = extendedRecord.version
            )
          )

          val updatedRecord = result.right.value.copy(
            version = 5
          )

          versionedDao.put(updatedRecord)

          assertTableHasItem(
            id = updatedRecord.id,
            extendedRecord.copy(version = 6),
            table
          )
      }
    }

    describe("DynamoDB failures") {
      it("fails if we exceed throughput limits on an UpdateItem") {
        assertUpdateFailsWithCorrectException(
          new ProvisionedThroughputExceededException(
            "You tried to write to DynamoDB too quickly!"
          )
        )
      }

      it("fails if we fail the conditional update") {
        assertUpdateFailsWithCorrectException(
          new ConditionalCheckFailedException(
            "true is not equal to false!"
          )
        )
      }

      it("fails if there's an unexpected error") {
        assertUpdateFailsWithCorrectException(
          new RuntimeException("AAAAAARGH!")
        )
      }

      it("fails if we exceed throughput limits on a GetItem") {
        assertGetFailsWithCorrectException(
          new ProvisionedThroughputExceededException(
            "You tried to read from DynamoDB too quickly!"
          )
        )
      }

      def assertGetFailsWithCorrectException(
        exceptionThrownByGetItem: Throwable): Assertion = {
          withLocalDynamoDbTable { table =>
            val mockDynamoDbClient = mock[AmazonDynamoDB]
            when(mockDynamoDbClient.getItem(any[GetItemRequest]))
              .thenThrow(exceptionThrownByGetItem)

            val failingDao = createDaoWith(mockDynamoDbClient, table)

            val result = failingDao.get(id = "123")
            val err = result.left.value.e
            err shouldBe exceptionThrownByGetItem
          }
        }

      def assertUpdateFailsWithCorrectException(
        exceptionThrownByUpdateItem: Throwable): Assertion = {
        withLocalDynamoDbTable { table =>
          val mockDynamoDbClient = mock[AmazonDynamoDB]
          when(mockDynamoDbClient.updateItem(any[UpdateItemRequest]))
            .thenThrow(exceptionThrownByUpdateItem)

          val record = Record(
            id = "testSource/b1111",
            data = "whatever",
            version = 1
          )

          val failingDao = createDaoWith(mockDynamoDbClient, table)

          val result = failingDao.put(record)
          val err = result.left.value.e
          err shouldBe exceptionThrownByUpdateItem
        }
      }
    }
  }

  def createDaoWith(dynamoClient: AmazonDynamoDB, table: Table): DynamoVersionedDao[String, Record] =
    DynamoVersionedDao[String, Record](
      dynamoClient = dynamoClient,
      dynamoConfig = createDynamoConfigWith(table)
    )
}
