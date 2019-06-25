package uk.ac.wellcome.storage.store.dynamo

import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType
import org.scanamo.auto._
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.dynamo.DynamoConfig
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table
import uk.ac.wellcome.storage.fixtures.{DynamoFixtures, S3Fixtures}
import uk.ac.wellcome.storage.generators.{ObjectLocationGenerators, Record, RecordGenerators}
import uk.ac.wellcome.storage.store.s3.{S3StreamStore, S3TypedStore}
import uk.ac.wellcome.storage.store.{HybridIndexedStoreEntry, HybridStoreEntry, VersionedStoreTestCases}
import uk.ac.wellcome.storage.{ObjectLocation, StoreReadError, StoreWriteError, Version}

class DynamoVersionedHybridStoreTest extends VersionedStoreTestCases[String, HybridStoreEntry[Record, Record],
  DynamoHybridStoreWithMaxima[Record, Record]]
  with RecordGenerators
  with ObjectLocationGenerators
  with S3Fixtures
  with DynamoFixtures {

  type IndexedStoreEntry = HybridIndexedStoreEntry[Version[String, Int], ObjectLocation, Record]

  override def createTable(table: Table): Table =
    createTableWithHashRangeKey(
      table = table,
      hashKeyName = "hashKey",
      rangeKeyName = "rangeKey",
      rangeKeyType = ScalarAttributeType.N
    )

  override def withFailingGetVersionedStore[R](initialEntries: Entries)(testWith: TestWith[VersionedStoreImpl, R]): R = {
    withVersionedStoreContext { storeContext =>
      initialEntries.map {
        case (k, v) => storeContext.put(k)(v).right.value
      }

      val vhs = new DynamoVersionedHybridStore[Record, Record](storeContext) {
        override def get(id: Version[String, Int]): ReadEither = {
          Left(StoreReadError(new Error("BOOM!")))
        }
      }

      testWith(vhs)
    }
  }

  override def withFailingPutVersionedStore[R](initialEntries: Entries)(testWith: TestWith[VersionedStoreImpl, R]): R = {
    withVersionedStoreContext { storeContext =>
      initialEntries.map {
        case (k, v) => storeContext.put(k)(v).right.value
      }

      val vhs = new DynamoVersionedHybridStore[Record, Record](storeContext) {
        override def put(id: Version[String, Int])(t: HybridStoreEntry[Record, Record]): WriteEither = {
          Left(StoreWriteError(new Error("BOOM!")))
        }
      }

      testWith(vhs)
    }
  }

  override def createIdent: String = randomAlphanumeric

  override def createT: HybridStoreEntry[Record, Record] = HybridStoreEntry(createRecord, createRecord)

  override def withVersionedStoreImpl[R](initialEntries: Entries, storeContext: DynamoHybridStoreWithMaxima[Record, Record])(testWith: TestWith[VersionedStoreImpl, R]): R = {
    initialEntries.map {
      case (k,v) => storeContext.put(k)(v).right.value
    }

    testWith(new DynamoVersionedHybridStore[Record, Record](storeContext))
  }

  override def withVersionedStoreContext[R](testWith: TestWith[DynamoHybridStoreWithMaxima[Record, Record], R]): R = {
    withLocalS3Bucket { bucket =>
      withLocalDynamoDbTable { table =>

        val streamStore = new S3StreamStore()
        val typedStore = new S3TypedStore[Record]()(codec, streamStore)

        val dynamoConfig = DynamoConfig(table.name, table.index)

        val indexedStore =
          new DynamoHashRangeStore[String, Int, IndexedStoreEntry](dynamoConfig)

        val prefix = createObjectLocationPrefixWith(bucket.name)

        testWith(new DynamoHybridStoreWithMaxima[Record, Record](prefix)(indexedStore, typedStore))
      }
    }
  }
}
