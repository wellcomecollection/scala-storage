package uk.ac.wellcome.storage.store.dynamo

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.{ConditionalCheckFailedException, ScalarAttributeType}
import org.scalatest.OptionValues
import uk.ac.wellcome.storage.dynamo.DynamoHashEntry
import uk.ac.wellcome.storage.generators.Record
import org.scanamo.{DynamoFormat, Table => ScanamoTable}
import uk.ac.wellcome.storage.Version
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table
import org.scanamo.syntax._

import org.scanamo.auto._

class DynamoHashWritableTest extends DynamoWritableTestCases[DynamoHashEntry[String, Int, Record]] with OptionValues {
  type HashEntry = DynamoHashEntry[String, Int, Record]

  class TestHashWritable(
                          val client: AmazonDynamoDB,
                          val table: ScanamoTable[HashEntry]
                        )(
                          implicit val formatV: DynamoFormat[Int]
                        ) extends DynamoHashWritable[HashKey, Int, Record]

  override def createDynamoWritableWith(table: Table, initialEntries: Set[HashEntry] = Set.empty): DynamoWritableStub =  {
    scanamo.exec(ScanamoTable[HashEntry](table.name).putAll(initialEntries))

    new TestHashWritable(dynamoClient, ScanamoTable[HashEntry](table.name))
  }

  override def getRecord(table: Table)(hashKey: HashKey, v: Int): Record =
    scanamo.exec(
      ScanamoTable[HashEntry](table.name).get('hashKey -> hashKey)
    ).value.right.value.payload

  override def createEntry(hashKey: String, v: Int, record: Record): HashEntry =
    DynamoHashEntry(hashKey, v, record)

  override def createTable(table: Table): Table =
    createTableWithHashKey(table, keyName = "hashKey")

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

  describe("fails if the table definition is wrong") {
    it("hash key name is wrong") {
      assertErrorsOnBadKeyName(
        table => createTableWithHashKey(table, keyName = "wrong")
      )
    }

    it("hash key is the wrong type") {
      assertErrorsOnBadKeyType(
        table => createTableWithHashKey(
          table,
          keyName = "hashKey",
          keyType = ScalarAttributeType.N
        )
      )
    }
  }
}
