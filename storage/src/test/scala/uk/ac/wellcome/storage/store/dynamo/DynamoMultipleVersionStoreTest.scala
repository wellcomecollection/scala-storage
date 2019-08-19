package uk.ac.wellcome.storage.store.dynamo

import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult
import org.scanamo.auto._
import org.scanamo.{Table => ScanamoTable}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.{StoreReadError, StoreWriteError, Version}
import uk.ac.wellcome.storage.dynamo.DynamoHashRangeEntry
import uk.ac.wellcome.storage.fixtures.DynamoFixtures
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table
import uk.ac.wellcome.storage.generators.{Record, RecordGenerators}
import uk.ac.wellcome.storage.store._

class DynamoMultipleVersionStoreTest
  extends VersionedStoreWithoutOverwriteTestCases[String, Record, Table]
    with RecordGenerators
    with DynamoFixtures {

  override def createIdent: String = randomAlphanumeric
  override def createT: Record = createRecord

  type DynamoStoreStub = DynamoMultipleVersionStore[String, Record]

  override def createTable(table: Table): Table =
    createTableWithHashRangeKey(table)

  private def insertEntries(table: Table)(entries: Map[Version[String, Int], Record]): Seq[BatchWriteItemResult] = {
    val scanamoTable = new ScanamoTable[DynamoHashRangeEntry[String, Int, Record]](table.name)

    val rows = entries.map {
      case (Version(id, version), payload) =>
        DynamoHashRangeEntry(id, version, payload)
    }

    debug(s"Inserting rows: $rows")

    scanamo.exec(scanamoTable.putAll(rows.toSet))
  }

  override def withVersionedStoreImpl[R](initialEntries: Map[Version[String, Int], Record])(testWith: TestWith[VersionedStoreImpl, R]): R =
    withLocalDynamoDbTable { table =>
      val store = new DynamoStoreStub(
        config = createDynamoConfigWith(table)
      )

    insertEntries(table)(initialEntries)

    testWith(store)
  }

  override def withVersionedStoreImpl[R](initialEntries: Entries, storeContext: Table)(testWith: TestWith[VersionedStoreImpl, R]): R = {
    val store = new DynamoStoreStub(
      config = createDynamoConfigWith(storeContext)
    )

    insertEntries(storeContext)(initialEntries)

    testWith(store)
  }


  override def withVersionedStoreContext[R](testWith: TestWith[Table, R]): R =
    withLocalDynamoDbTable { table => testWith(table) }

  override def withFailingGetVersionedStore[R](initialEntries: Map[Version[String, Int], Record])(testWith: TestWith[VersionedStoreImpl, R]): R =
    withLocalDynamoDbTable { table =>
      val store = new DynamoStoreStub(
        config = createDynamoConfigWith(table)
      ) {
        override def get(id: Version[String, Int]): ReadEither = {
          Left(StoreReadError(new Error("BOOM!")))
        }
      }

      insertEntries(table)(initialEntries)

      testWith(store)
    }

  override def withFailingPutVersionedStore[R](initialEntries: Map[Version[String, Int], Record])(testWith: TestWith[VersionedStoreImpl, R]): R =
    withLocalDynamoDbTable { table =>
      val store = new DynamoStoreStub(
        config = createDynamoConfigWith(table)
      ) {
        override def put(id: Version[String, Int])(t: Record): WriteEither = {
          Left(StoreWriteError(new Error("BOOM!")))
        }
      }

      insertEntries(table)(initialEntries)

      testWith(store)
    }

  override def withStoreContext[R](testWith: TestWith[Table, R]): R =
    withVersionedStoreContext(testWith)

  override def withNamespace[R](testWith: TestWith[String, R]): R = testWith(randomAlphanumeric)

  override def createId(implicit namespace: String): Version[String, Int] = Version(randomAlphanumeric, 0)

  override def withStoreImpl[R](initialEntries: Map[Version[String, Int], Record], storeContext: Table)(testWith: TestWith[StoreImpl, R]): R = withVersionedStoreImpl(initialEntries, storeContext)(testWith)

}
