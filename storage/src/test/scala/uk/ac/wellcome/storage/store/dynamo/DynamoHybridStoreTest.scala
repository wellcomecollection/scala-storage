package uk.ac.wellcome.storage.store.dynamo

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.dynamo.DynamoConfig
import uk.ac.wellcome.storage.fixtures.{DynamoFixtures, S3Fixtures}
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.generators.{ObjectLocationGenerators, Record, RecordGenerators}
import uk.ac.wellcome.storage.{ObjectLocation, StoreReadError, StoreWriteError, Version}
import uk.ac.wellcome.storage.store.{HybridIndexedStoreEntry, HybridStoreEntry, HybridStoreTestCases, TypedStoreEntry}
import uk.ac.wellcome.storage.store.s3.{S3StreamStore, S3TypedStore}

class DynamoHybridStoreTest
  extends HybridStoreTestCases[Version[String, Int], ObjectLocation, Record, Record, Bucket, S3TypedStore[Record], DynamoHashStore[String, Int, HybridIndexedStoreEntry[Version[String, Int], ObjectLocation, Record]], DynamoHybridStore[Record, Record]]
    with RecordGenerators with S3Fixtures with ObjectLocationGenerators with DynamoFixtures {

  override def createTypedStoreId: ObjectLocation = createObjectLocation

  override def createMetadata: Record = createRecord

  override def createT: HybridStoreEntry[Record, Record] = HybridStoreEntry(createRecord, createMetadata)


  override def withNamespace[R](testWith: TestWith[Bucket, R]): R =
    withLocalS3Bucket { bucket => testWith(bucket) }

  override def createId(implicit namespace: Bucket): Version[String, Int] =
    Version(randomAlphanumeric, 0)

  override def createHybridStoreWith(context: (
      S3TypedStore[Record],
      DynamoHashStore[String, Int, HybridIndexedStoreEntry[Version[String, Int], ObjectLocation, Record]]
    )
  ): DynamoHybridStore[Record, Record] = {

    implicit val (typedStore, indexedStore) = context

    new DynamoHybridStore[Record, Record]()

    ???
  }

  override def createBrokenPutTypedStore: S3TypedStore[Record] = {
    implicit val streamStore = new S3StreamStore()

    new S3TypedStore[Record]() {
      override def put(id: ObjectLocation)(entry: TypedStoreEntry[Record]): WriteEither = {
        Left(StoreWriteError(new Error("BOOM!")))
      }
    }
  }

  override def createBrokenPutIndexedStore: DynamoHashStore[String, Int, HybridIndexedStoreEntry[Version[String, Int], ObjectLocation, Record]] = {
    val config = DynamoConfig()

    new DynamoHashStore[String, Int, HybridIndexedStoreEntry[Version[String, Int], ObjectLocation, Record]](config) {
      override def put(id: Version[String, Int])(t: HybridIndexedStoreEntry[Version[String, Int], ObjectLocation, Record]): WriteEither = {
        Left(StoreWriteError(new Error("BOOM!")))
      }
    }
  }

  override def createBrokenGetTypedStore: S3TypedStore[Record] = {
    implicit val streamStore = new S3StreamStore()

    new S3TypedStore[Record]() {
      override def get(id: ObjectLocation): ReadEither = {
        Left(StoreReadError(new Error("BOOM!")))
      }
    }
  }

  override def createBrokenGetIndexedStore: DynamoHashStore[String, Int, HybridIndexedStoreEntry[Version[String, Int], ObjectLocation, Record]] = {
    val config = DynamoConfig()

    new DynamoHashStore[String, Int, HybridIndexedStoreEntry[Version[String, Int], ObjectLocation, Record]](config) {
      override def get(id: Version[String, Int]): ReadEither = {
        Left(StoreReadError(new Error("BOOM!")))
      }
    }
  }

  override def withStoreContext[R](testWith: TestWith[(S3TypedStore[Record], DynamoHashStore[String, Int, HybridIndexedStoreEntry[Version[String, Int], ObjectLocation, Record]]), R]): R = ???

}
