package uk.ac.wellcome.storage.store.dynamo

import org.scanamo.auto._
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.dynamo.DynamoConfig
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table
import uk.ac.wellcome.storage.fixtures.{DynamoFixtures, S3Fixtures}
import uk.ac.wellcome.storage.generators.{MetadataGenerators, Record, RecordGenerators}
import uk.ac.wellcome.storage.store.s3.{S3StreamStore, S3TypedStore}
import uk.ac.wellcome.storage.store.{HybridIndexedStoreEntry, HybridStoreEntry, VersionedStoreTestCases}
import uk.ac.wellcome.storage.{S3ObjectLocation, StoreReadError, StoreWriteError, Version}

class DynamoVersionedHybridStoreTest extends VersionedStoreTestCases[String, HybridStoreEntry[Record, Map[String, String]],
  DynamoHybridStoreWithMaxima[String, Int, Record, Map[String, String]]]
  with RecordGenerators
  with S3Fixtures
  with MetadataGenerators
  with DynamoFixtures {

  type DynamoVersionedStoreImpl = DynamoVersionedHybridStore[String, Int, Record, Map[String, String]]
  type IndexedStoreEntry = HybridIndexedStoreEntry[S3ObjectLocation, Map[String, String]]

  override def createTable(table: Table): Table =
    createTableWithHashRangeKey(table)

  override def withFailingGetVersionedStore[R](initialEntries: Entries)(testWith: TestWith[VersionedStoreImpl, R]): R = {
    withVersionedStoreContext { storeContext =>
      initialEntries.map {
        case (k, v) => storeContext.put(k)(v).right.value
      }

      val vhs = new DynamoVersionedStoreImpl(storeContext) {
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

      val vhs = new DynamoVersionedStoreImpl(storeContext) {
        override def put(id: Version[String, Int])(t: HybridStoreEntry[Record, Map[String, String]]): WriteEither = {
          Left(StoreWriteError(new Error("BOOM!")))
        }
      }

      testWith(vhs)
    }
  }

  override def createIdent: String = randomAlphanumeric

  override def createT: HybridStoreEntry[Record, Map[String, String]] = HybridStoreEntry(createRecord, createValidMetadata)

  override def withVersionedStoreImpl[R](
    initialEntries: Entries, storeContext: DynamoHybridStoreWithMaxima[String, Int, Record, Map[String, String]])(
    testWith: TestWith[VersionedStoreImpl, R]): R = {
    initialEntries.map {
      case (k,v) => storeContext.put(k)(v).right.value
    }

    testWith(new DynamoVersionedHybridStore[String, Int, Record, Map[String, String]](storeContext))
  }

  override def withVersionedStoreContext[R](testWith: TestWith[DynamoHybridStoreWithMaxima[String, Int, Record, Map[String, String]], R]): R = {
    withLocalS3Bucket { bucket =>
      withLocalDynamoDbTable { table =>
        implicit val streamStore: S3StreamStore = new S3StreamStore()
        val typedStore = new S3TypedStore[Record]()

        val dynamoConfig = DynamoConfig(table.name, table.index)

        val indexedStore =
          new DynamoHashRangeStore[String, Int, IndexedStoreEntry](dynamoConfig)

        val prefix = createS3ObjectLocationPrefixWith(bucket)

        testWith(new DynamoHybridStoreWithMaxima[String, Int, Record, Map[String, String]](prefix)(indexedStore, typedStore))
      }
    }
  }

  override def withStoreImpl[R](
    initialEntries: Map[Version[String, Int], HybridStoreEntry[Record, Map[String, String]]],
    storeContext: DynamoHybridStoreWithMaxima[String, Int, Record, Map[String, String]])(
      testWith: TestWith[StoreImpl, R]): R =
    withVersionedStoreImpl(initialEntries, storeContext)(testWith)

  override def withStoreContext[R](testWith: TestWith[DynamoHybridStoreWithMaxima[String, Int, Record, Map[String, String]], R]): R =
    withVersionedStoreContext(testWith)

  override def withNamespace[R](testWith: TestWith[String, R]): R = testWith(randomAlphanumeric)

  override def createId(implicit namespace: String): Version[String, Int] = Version(randomAlphanumeric, 0)
}
