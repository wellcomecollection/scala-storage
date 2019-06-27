package uk.ac.wellcome.storage.store.dynamo

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.{AmazonDynamoDBException, ConditionalCheckFailedException, ScalarAttributeType}
import org.scalatest.OptionValues
import org.scanamo.auto._
import org.scanamo.syntax._
import uk.ac.wellcome.storage.dynamo.DynamoHashEntry
import uk.ac.wellcome.storage.generators.{Record, RecordGenerators}
import org.scanamo.{DynamoFormat, Table => ScanamoTable}
import uk.ac.wellcome.storage.Version
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table

class DynamoHashWritableTest extends DynamoWritableTestCases[String, Record, DynamoHashEntry[String, Int, Record]] with OptionValues with RecordGenerators {
  type HashEntry = DynamoHashEntry[String, Int, Record]

  def createId: String = randomAlphanumeric
  def createT: Record = createRecord

  class TestHashWritable(
    val client: AmazonDynamoDB,
    val table: ScanamoTable[HashEntry]
  )(
    implicit val formatV: DynamoFormat[Int]
  ) extends DynamoHashWritable[String, Int, Record]

  override def createDynamoWritableWith(table: Table, initialEntries: Set[HashEntry] = Set.empty): DynamoWritableStub =  {
    scanamo.exec(ScanamoTable[HashEntry](table.name).putAll(initialEntries))

    new TestHashWritable(dynamoClient, ScanamoTable[HashEntry](table.name))
  }

  override def getT(table: Table)(hashKey: String, v: Int): Record =
    scanamo.exec(
      ScanamoTable[HashEntry](table.name).get('id -> hashKey)
    ).value.right.value.payload

  override def createEntry(hashKey: String, v: Int, record: Record): HashEntry =
    DynamoHashEntry(hashKey, v, record)

  override def createTable(table: Table): Table =
    createTableWithHashKey(table)

  describe("DynamoHashWritable") {
    it("fails to overwrite a new version with an old version") {
      val hashKey = randomAlphanumeric
      val olderRecord = createRecord
      val newerRecord = createRecord

      withLocalDynamoDbTable { table =>
        val writable = createDynamoWritableWith(table, initialEntries = Set(
          createEntry(hashKey, 2, newerRecord)
        ))

        val result = writable.put(id = Version(hashKey, 1))(olderRecord)

        val err = result.left.value
        err.e shouldBe a[ConditionalCheckFailedException]
        err.e.getMessage should startWith("The conditional request failed")
      }
    }

    it("fails if the partition key is too long") {
      // Maximum length of an partition key is 2048 bytes as of 25/06/2019
      // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html#limits-partition-sort-keys

      val hashKey = randomStringOfByteLength(2049)()

      val record = createRecord

      withLocalDynamoDbTable { table =>
        val writable = createDynamoWritableWith(table, initialEntries = Set.empty)
        val result = writable.put(id = Version(hashKey, 1))(record)

        val err = result.left.value

        err.e shouldBe a[AmazonDynamoDBException]
        err.e.getMessage should include("Hash primary key values must be under 2048 bytes")
      }
    }

    describe("fails if the table definition is wrong") {
      it("hash key name is wrong") {
        assertErrorsOnBadKeyName(
          table => createTableWithHashKey(table, keyName = "wrong")
        )
      }

      it("hash key is the wrong type") {
        assertErrorsOnBadKeyType(
          table => createTableWithHashKey(table, keyType = ScalarAttributeType.N)
        )
      }
    }
  }
}
