package uk.ac.wellcome.storage.maxima.dynamo

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.model.{
  AmazonDynamoDBException,
  ResourceNotFoundException,
  ScalarAttributeType
}
import org.scanamo.{DynamoFormat, Table => ScanamoTable}
import org.scanamo.auto._
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.MaximaReadError
import uk.ac.wellcome.storage.dynamo.DynamoHashRangeEntry
import uk.ac.wellcome.storage.fixtures.DynamoFixtures
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table
import uk.ac.wellcome.storage.generators.Record
import uk.ac.wellcome.storage.maxima.MaximaTestCases
import uk.ac.wellcome.storage.{IdentityKey, Version}

class DynamoHashRangeMaximaTest extends MaximaTestCases with DynamoFixtures {
  type Entry = DynamoHashRangeEntry[IdentityKey, Int, Record]

  def withMaxima[R](table: Table,
                    initialEntries: Map[Version[IdentityKey, Int], Record] =
                      Map.empty)(testWith: TestWith[MaximaStub, R]): R = {
    val dynamoEntries = initialEntries.map {
      case (id, record) => DynamoHashRangeEntry(id.id, id.version, record)
    }.toSeq

    putTableItems(dynamoEntries, table)

    class DynamoMaxima(dynamoTable: Table)(
      implicit val formatHashKey: DynamoFormat[IdentityKey],
      implicit val formatRangeKey: DynamoFormat[Int],
      implicit val format: DynamoFormat[Entry]
    ) extends DynamoHashRangeMaxima[IdentityKey, Int, Entry] {
      val table = ScanamoTable[Entry](dynamoTable.name)

      val client: AmazonDynamoDB = dynamoClient
    }

    testWith(new DynamoMaxima(table))
  }

  override def withMaxima[R](
    initialEntries: Map[Version[IdentityKey, Int], Record])(
    testWith: TestWith[MaximaStub, R]): R =
    withLocalDynamoDbTable { table =>
      withMaxima(table, initialEntries) { maxima =>
        testWith(maxima)
      }
    }

  def createTable(table: Table): Table =
    createTableWithHashRangeKey(table)

  describe("DynamoHashRangeMaxima") {
    it("fails if DynamoDB has an error") {
      withMaxima(nonExistentTable) { maxima =>
        val result = maxima.max(createIdentityKey)

        val err = result.left.value
        err shouldBe a[MaximaReadError]
        err.e shouldBe a[ResourceNotFoundException]
      }
    }

    describe("if the table has the wrong structure") {
      it("when the hash key name is wrong") {
        def createWrongTable(table: Table): Table =
          createTableWithHashKey(table, keyName = "wrong")

        withSpecifiedTable(createWrongTable) { table =>
          withMaxima(table) { maxima =>
            val result = maxima.max(createIdentityKey)

            val err = result.left.value
            err shouldBe a[MaximaReadError]
            err.e shouldBe a[AmazonDynamoDBException]
            err.e.getMessage should startWith(
              "Query condition missed key schema element")
          }
        }
      }

      it("when the hash key type is wrong") {
        def createWrongTable(table: Table): Table =
          createTableWithHashKey(table, keyType = ScalarAttributeType.N)

        withSpecifiedTable(createWrongTable) { table =>
          withMaxima(table) { maxima =>
            val result = maxima.max(createIdentityKey)

            val err = result.left.value
            err shouldBe a[MaximaReadError]
            err.e shouldBe a[AmazonDynamoDBException]
            err.e.getMessage should include(
              "Condition parameter type does not match schema type")
          }
        }
      }

      it("when the range key name is wrong") {
        def createWrongTable(table: Table): Table =
          createTableWithHashRangeKey(table, rangeKeyName = "wrong")

        case class WrongEntry(id: IdentityKey, wrong: Int, record: Record)

        val id = createIdentityKey

        withSpecifiedTable(createWrongTable) { table =>
          scanamo.exec(
            ScanamoTable[WrongEntry](table.name).putAll(
              Set(
                WrongEntry(id, wrong = 1, record = createRecord),
                WrongEntry(id, wrong = 2, record = createRecord)
              )
            ))

          withMaxima(table) { maxima =>
            val result = maxima.max(id)

            val err = result.left.value
            err shouldBe a[MaximaReadError]
            err.e shouldBe a[Error]
            err.e.getMessage should startWith(
              "DynamoReadError: InvalidPropertiesError")
          }
        }
      }

      it("when the range key type is wrong") {
        case class WrongEntry(id: IdentityKey, version: String, record: Record)

        val id = createIdentityKey

        def createWrongTable(table: Table): Table =
          createTableWithHashRangeKey(
            table,
            rangeKeyType = ScalarAttributeType.S)

        withSpecifiedTable(createWrongTable) { table =>
          scanamo.exec(
            ScanamoTable[WrongEntry](table.name).putAll(
              Set(
                WrongEntry(
                  id,
                  version = randomAlphanumeric,
                  record = createRecord),
                WrongEntry(
                  id,
                  version = randomAlphanumeric,
                  record = createRecord)
              )
            ))

          withMaxima(table) { maxima =>
            val result = maxima.max(id)

            val err = result.left.value
            err shouldBe a[MaximaReadError]
            err.e shouldBe a[Error]
            err.e.getMessage should include(
              "DynamoReadError: InvalidPropertiesError")
          }
        }
      }
    }
  }
}
