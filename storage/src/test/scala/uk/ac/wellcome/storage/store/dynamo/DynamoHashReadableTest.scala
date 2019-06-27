package uk.ac.wellcome.storage.store.dynamo

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType
import org.scanamo.{DynamoFormat, Table => ScanamoTable}
import uk.ac.wellcome.storage.dynamo.DynamoHashEntry
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table
import uk.ac.wellcome.storage.generators.Record
import uk.ac.wellcome.storage.{Identified, NoVersionExistsError, Version}
import org.scanamo.auto._

class DynamoHashReadableTest extends DynamoReadableTestCases[String, DynamoHashEntry[String, Int, Record]] {
  type HashEntry = DynamoHashEntry[String, Int, Record]

  override def createDynamoReadableWith(table: Table, initialEntries: Set[DynamoHashEntry[String, Int, Record]] = Set.empty): DynamoReadableStub = {
    class DynamoHashReadableImpl(
                                  val client: AmazonDynamoDB,
                                  val table: ScanamoTable[HashEntry]
                                )(
                                  implicit val formatHashKey: DynamoFormat[String],
                                  implicit val format: DynamoFormat[HashEntry]
                                ) extends DynamoHashReadable[HashKey, Int, Record]

    scanamo.exec(ScanamoTable[HashEntry](table.name).putAll(initialEntries))

    new DynamoHashReadableImpl(dynamoClient, ScanamoTable[HashEntry](table.name))
  }

  override def createTable(table: Table): Table = createTableWithHashKey(table)

  override def createEntry(hashKey: String, v: Int, record: Record): HashEntry =
    DynamoHashEntry(hashKey, v, record)

  describe("DynamoHashReadable") {
    describe("fails if the table definition is wrong") {
      it("hash key name is wrong") {
        assertErrorsOnBadKeyName(table =>
          createTableWithHashKey(table, keyName = "wrong")
        )
      }

      it("hash key is the wrong type") {
        assertErrorsOnBadKeyType(table =>
          createTableWithHashKey(table, keyType = ScalarAttributeType.N)
        )
      }
    }

    it("finds a row with matching hashKey and version") {
      val id = randomAlphanumeric
      val record = createRecord

      val initialEntries = Set(
        createEntry(id, v = 1, record),
      )

      withLocalDynamoDbTable { table =>
        val readable = createDynamoReadableWith(table, initialEntries)

        readable.get(Version(id, 1)).right.value shouldBe Identified(Version(id, 1), record)
      }
    }

    it("fails if there is a row with matching hashKey but wrong version") {
      val id = randomAlphanumeric
      val record = createRecord

      val initialEntries = Set(
        createEntry(id, v = 2, record),
      )

      withLocalDynamoDbTable { table =>
        val readable = createDynamoReadableWith(table, initialEntries)

        readable.get(Version(id, 1)).left.value shouldBe a[NoVersionExistsError]
      }
    }
  }
}