package uk.ac.wellcome.storage.store.dynamo

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType
import org.scalatest.OptionValues
import org.scanamo.{DynamoFormat, Table => ScanamoTable}
import org.scanamo.auto._
import org.scanamo.syntax._
import uk.ac.wellcome.storage.dynamo.DynamoHashRangeEntry
import uk.ac.wellcome.storage.generators.{Record, RecordGenerators}
import uk.ac.wellcome.storage.Version
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table

class DynamoHashRangeWritableTest
  extends DynamoWritableTestCases[String, Record, DynamoHashRangeEntry[String, Int, Record]]
    with OptionValues
    with RecordGenerators {
  type HashRangeEntry = DynamoHashRangeEntry[String, Int, Record]

  def createId: String = randomAlphanumeric
  def createT: Record = createRecord

  class HashRangeWritableImpl(
    val client: AmazonDynamoDB,
    val table: ScanamoTable[HashRangeEntry]
  )(
    implicit val formatRangeKey: DynamoFormat[Int]
  ) extends DynamoHashRangeWritable[String, Int, Record]

  override def createDynamoWritableWith(table: Table, initialEntries: Set[HashRangeEntry] = Set.empty): DynamoWritableStub =  {
    scanamo.exec(ScanamoTable[HashRangeEntry](table.name).putAll(initialEntries))

    new HashRangeWritableImpl(dynamoClient, ScanamoTable[HashRangeEntry](table.name))
  }

  override def getT(table: Table)(hashKey: String, v: Int): Record =
  scanamo.exec(
    ScanamoTable[HashRangeEntry](table.name).get('hashKey -> hashKey and 'rangeKey -> v)
  ).value.right.value.payload

  override def createEntry(hashKey: String, v: Int, record: Record): HashRangeEntry =
    DynamoHashRangeEntry(hashKey, v, record)

  override def createTable(table: Table): Table =
    createTableWithHashRangeKey(
      table = table,
      hashKeyName = "hashKey",
      rangeKeyName = "rangeKey",
      rangeKeyType = ScalarAttributeType.N
    )

  it("allows putting the same hash key at multiple versions") {
    val hashKey = randomAlphanumeric

    withLocalDynamoDbTable { table =>
      val writable = createDynamoWritableWith(table, initialEntries = Set(
        createEntry(hashKey, 2, createRecord)
      ))

      writable.put(id = Version(hashKey, 1))(createRecord) shouldBe a[Right[_, _]]
      writable.put(id = Version(hashKey, 3))(createRecord) shouldBe a[Right[_, _]]

      scanamo.exec(ScanamoTable[HashRangeEntry](table.name).scan()) should have size 3
    }
  }

  describe("fails if the table definition is wrong") {
    it("hash key name is wrong") {
      assertErrorsOnBadKeyName(
        table =>
          createTableWithHashRangeKey(
            table,
            hashKeyName = "wrong",
            hashKeyType = ScalarAttributeType.S,
            rangeKeyName = "rangeKey",
            rangeKeyType = ScalarAttributeType.N
          )
      )
    }

    it("hash key is the wrong type") {
      assertErrorsOnBadKeyType(
        table =>
          createTableWithHashRangeKey(
            table,
            hashKeyName = "hashKey",
            hashKeyType = ScalarAttributeType.N,
            rangeKeyName = "rangeKey",
            rangeKeyType = ScalarAttributeType.N
          )
      )
    }

    it("range key name is wrong") {
      assertErrorsOnBadKeyName(
        table =>
          createTableWithHashRangeKey(
            table,
            hashKeyName = "hashKey",
            hashKeyType = ScalarAttributeType.S,
            rangeKeyName = "wrong",
            rangeKeyType = ScalarAttributeType.N
          )
      )
    }

    it("range key is the wrong type") {
      assertErrorsOnBadKeyType(
        table =>
          createTableWithHashRangeKey(
            table,
            hashKeyName = "hashKey",
            hashKeyType = ScalarAttributeType.S,
            rangeKeyName = "rangeKey",
            rangeKeyType = ScalarAttributeType.S
          )
      )
    }
  }
}
