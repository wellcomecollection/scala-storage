package uk.ac.wellcome.storage.store.memory

import java.util.UUID

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.generators.{Record, RecordGenerators}
import uk.ac.wellcome.storage.store._

class MemoryHybridStoreTest
  extends HybridStoreTestCases[UUID, String, Record, Record, String,
    MemoryTypedStore[String, Record],
    MemoryStore[UUID, HybridIndexedStoreEntry[UUID, String, Record]], MemoryHybridStore[UUID, Record, Record]]
    with RecordGenerators {

  type IndexedMemoryStore = MemoryStore[UUID, HybridIndexedStoreEntry[UUID, String, Record]]

  override def createHybridStoreWith(context: (MemoryTypedStore[String, Record], IndexedMemoryStore)): MemoryHybridStore[UUID, Record, Record] = {
    val (typedStore, indexedStore) = context

    new MemoryHybridStore[UUID, Record, Record]()(typedStore, indexedStore, codec)
  }

  override def withStoreContext[R](testWith: TestWith[(MemoryTypedStore[String, Record], IndexedMemoryStore), R]): R = {
    val streamStore = MemoryStreamStore[String]()

    val typedStore =
      new MemoryTypedStore[String, Record]()(streamStore, codec)

    val indexedStore =
      new MemoryStore[UUID, HybridIndexedStoreEntry[UUID, String, Record]](Map.empty)

    val context = (typedStore, indexedStore)

    testWith(context)
  }


  override def createBrokenGetTypedStore: MemoryTypedStore[String, Record] = {
    val streamStore = MemoryStreamStore[String]()

    new MemoryTypedStore[String, Record]()(streamStore, codec) {
      override def get(id: String): ReadEither = {
        Left(StoreReadError(new Error("BOOM!")))
      }
    }
  }

  override def createBrokenPutTypedStore: MemoryTypedStore[String, Record] = {
    val streamStore = MemoryStreamStore[String]()

    new MemoryTypedStore[String, Record]()(streamStore, codec) {
      override def put(id: String)(entry: TypedStoreEntry[Record]): WriteEither = {
        Left(StoreWriteError(new Error("BOOM!")))
      }
    }
  }

  override def createBrokenGetIndexedStore: IndexedMemoryStore = {
    new MemoryStore[UUID, HybridIndexedStoreEntry[UUID, String, Record]](Map.empty) {
      override def get(id: UUID): ReadEither = {
        Left(StoreReadError(new Error("BOOM!")))
      }
    }
  }

  override def createBrokenPutIndexedStore: IndexedMemoryStore = {
    new MemoryStore[UUID, HybridIndexedStoreEntry[UUID, String, Record]](Map.empty) {
      override def put(id: UUID)(t: HybridIndexedStoreEntry[UUID, String, Record]): Either[WriteError, Identified[UUID, HybridIndexedStoreEntry[UUID, String, Record]]] = {
        Left(StoreWriteError(new Error("BOOM!")))
      }
    }
  }

  override def createTypedStoreId: String = randomAlphanumeric
  override def createMetadata: Record = createRecord
  override def createT: HybridStoreEntry[Record, Record] = {
    HybridStoreEntry(createRecord, createMetadata)
  }

  override def withNamespace[R](testWith: TestWith[String, R]): R = testWith(randomAlphanumeric)
  override def createId(implicit namespace: String): UUID = UUID.randomUUID()
}
