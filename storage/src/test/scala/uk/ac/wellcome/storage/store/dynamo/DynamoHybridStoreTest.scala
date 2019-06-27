package uk.ac.wellcome.storage.store.dynamo

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.{ObjectLocation, StoreReadError, StoreWriteError, Version}
import uk.ac.wellcome.storage.fixtures.DynamoFixtures.Table
import uk.ac.wellcome.storage.fixtures.S3Fixtures.Bucket
import uk.ac.wellcome.storage.generators.Record
import uk.ac.wellcome.storage.store.HybridIndexedStoreEntry
import org.scanamo.auto._

class DynamoHybridStoreTest extends DynamoHybridStoreTestCases[
  DynamoHashStore[String, Int, HybridIndexedStoreEntry[Version[String, Int], ObjectLocation, Map[String, String]]]
  ] {
  override def createTable(table: Table): Table =
    createTableWithHashKey(table)

  override def withHybridStoreImpl[R](
    typedStore: S3TypedStoreImpl,
    indexedStore: DynamoIndexedStoreImpl)(testWith: TestWith[HybridStoreImpl, R])(implicit context: (Bucket, Table)): R = {
    implicit val underlyingTypedStore: S3TypedStoreImpl = typedStore
    implicit val underlyingIndexedStore: DynamoIndexedStoreImpl = indexedStore

    val hybridStore = new DynamoHybridStore[Record, Map[String, String]](createPrefix)

    testWith(hybridStore)
  }

  override def withIndexedStoreImpl[R](testWith: TestWith[DynamoIndexedStoreImpl, R])(implicit context: (Bucket, Table)): R = {
    val (_, table) = context

    testWith(
      new DynamoIndexedStoreImpl(
        config = createDynamoConfigWith(table)
      )
    )
  }

  override def withBrokenPutIndexedStoreImpl[R](testWith: TestWith[DynamoIndexedStoreImpl, R])(implicit context: (Bucket, Table)): R = {
    val (_, table) = context

    testWith(
      new DynamoIndexedStoreImpl(config = createDynamoConfigWith(table)) {
        override def put(id: Version[String, Int])(t: HybridIndexedStoreEntry[Version[String, Int], ObjectLocation, Map[String, String]]): WriteEither =
          Left(StoreWriteError(new Error("BOOM!")))
      }
    )
  }

  override def withBrokenGetIndexedStoreImpl[R](testWith: TestWith[DynamoIndexedStoreImpl, R])(implicit context: (Bucket, Table)): R = {
    val (_, table) = context

    testWith(
      new DynamoIndexedStoreImpl(config = createDynamoConfigWith(table)) {
        override def get(id: Version[String, Int]): ReadEither =
          Left(StoreReadError(new Error("BOOM!")))
      }
    )
  }
}
