package uk.ac.wellcome.storage.store.dynamo

import uk.ac.wellcome.fixtures.TestWith
import org.scanamo.auto._
import org.scanamo.{Table => ScanamoTable}
import uk.ac.wellcome.storage.{IdentityKey, Version}
import uk.ac.wellcome.storage.dynamo.DynamoHashEntry
import uk.ac.wellcome.storage.fixtures.DynamoFixtures
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table
import uk.ac.wellcome.storage.generators.{Record, RecordGenerators}
import uk.ac.wellcome.storage.maxima.MaximaTestCases
import uk.ac.wellcome.storage.store.StoreWithoutOverwritesTestCases

class DynamoHashStoreTest
  extends StoreWithoutOverwritesTestCases[Version[IdentityKey, Int], Record, String, Table]
    with MaximaTestCases
    with RecordGenerators
    with DynamoFixtures {
  def withDynamoHashStore[R](
    initialEntries: Map[Version[IdentityKey, Int], Record], table: Table)(
    testWith: TestWith[DynamoHashStore[IdentityKey, Int, Record], R]): R = {
    val dynamoEntries = initialEntries.map { case (id, record) =>
      DynamoHashEntry(id.id, id.version, record)
    }.toSet

    dynamoEntries.foreach { entry =>
      scanamo.exec(ScanamoTable[DynamoHashEntry[IdentityKey, Int, Record]](table.name).put(entry))
    }

    val store = new DynamoHashStore[IdentityKey, Int, Record](
      config = createDynamoConfigWith(table)
    )

    testWith(store)
  }

  override def withStoreImpl[R](initialEntries: Map[Version[IdentityKey, Int], Record], table: Table)(testWith: TestWith[StoreImpl, R]): R =
    withDynamoHashStore(initialEntries, table) { store =>
      testWith(store)
    }

  override def withStoreContext[R](testWith: TestWith[Table, R]): R =
    withLocalDynamoDbTable { table =>
      testWith(table)
    }

  override def createT: Record = createRecord

  override def withNamespace[R](testWith: TestWith[String, R]): R = testWith(randomAlphanumeric)

  override def createTable(table: Table): Table = createTableWithHashKey(table)

  override def createId(implicit namespace: String): Version[IdentityKey, Int] =
    Version(id = IdentityKey(randomAlphanumeric), version = 1)

  override def withMaxima[R](initialEntries: Map[Version[IdentityKey, Int], Record])(testWith: TestWith[MaximaStub, R]): R =
    withLocalDynamoDbTable { table =>
      withDynamoHashStore(initialEntries, table) { store =>
        testWith(store)
      }
    }
}
