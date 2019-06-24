package uk.ac.wellcome.storage.store.dynamo

import uk.ac.wellcome.fixtures.TestWith
import org.scanamo.auto._
import org.scanamo.{Table => ScanamoTable}
import uk.ac.wellcome.storage.Version
import uk.ac.wellcome.storage.dynamo.DynamoHashEntry
import uk.ac.wellcome.storage.fixtures.DynamoFixtures
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table
import uk.ac.wellcome.storage.generators.{Record, RecordGenerators}
import uk.ac.wellcome.storage.store.StoreWithoutOverwritesTestCases

class DynamoHashStoreTest extends StoreWithoutOverwritesTestCases[Version[String, Int], Record, String, Table] with RecordGenerators with DynamoFixtures {
  override def withStoreImpl[R](table: Table, initialEntries: Map[Version[String, Int], Record])(testWith: TestWith[StoreImpl, R]): R = {
    val dynamoEntries = initialEntries.map { case (id, record) =>
      DynamoHashEntry(id.id, id.version, record)
    }.toSet

    scanamo.exec(ScanamoTable[DynamoHashEntry[String, Int, Record]](table.name).putAll(dynamoEntries))

    val store = new DynamoHashStore[String, Int, Record](
      client = dynamoClient,
      dynamoConfig = createDynamoConfigWith(table)
    )

    testWith(store)
  }

  override def withStoreContext[R](testWith: TestWith[Table, R]): R =
    withLocalDynamoDbTable { table =>
      testWith(table)
    }

  override def createT: Record = createRecord

  override def withNamespace[R](testWith: TestWith[String, R]): R = testWith(randomAlphanumeric)

  override def createTable(table: Table): Table = createTableWithHashKey(table, keyName = "hashKey")

  override def createId(implicit namespace: String): Version[String, Int] =
    Version(id = randomAlphanumeric, version = 1)
}
