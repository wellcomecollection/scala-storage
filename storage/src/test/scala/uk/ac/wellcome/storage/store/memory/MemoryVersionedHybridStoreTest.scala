package uk.ac.wellcome.storage.store.memory

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.{StoreReadError, StoreWriteError, Version}
import uk.ac.wellcome.storage.generators.{Record, RecordGenerators}
import uk.ac.wellcome.storage.maxima.memory.MemoryMaxima
import uk.ac.wellcome.storage.store.{HybridIndexedStoreEntry, HybridStoreEntry, VersionedStoreWithOverwriteTestCases}

class MemoryVersionedHybridStoreTest
  extends VersionedStoreWithOverwriteTestCases[String, HybridStoreEntry[Record, Record],
    MemoryHybridStoreWithMaxima[String, Record, Record]]
    with RecordGenerators {

  type IndexedStoreEntry = HybridIndexedStoreEntry[String, Record]

  override def withFailingGetVersionedStore[R](initialEntries: Entries)(testWith: TestWith[VersionedStoreImpl, R]): R = {
    withVersionedStoreContext { storeContext =>

      initialEntries.map {
        case (k, v) => storeContext.put(k)(v).right.value
      }

      val versionedHybridStore = new MemoryVersionedHybridStore[String, Record, Record](storeContext) {
        override def get(id: Version[String, Int]): ReadEither = {
          Left(StoreReadError(new Error("BOOM!")))
        }
      }

      testWith(versionedHybridStore)
    }
  }

  override def withFailingPutVersionedStore[R](initialEntries: Entries)(testWith: TestWith[VersionedStoreImpl, R]): R = {
    withVersionedStoreContext { storeContext =>

      initialEntries.map {
        case (k, v) => storeContext.put(k)(v).right.value
      }

      val versionedHybridStore = new MemoryVersionedHybridStore[String, Record, Record](storeContext) {
        override def put(id: Version[String, Int])(t: HybridStoreEntry[Record, Record]): WriteEither = {
          Left(StoreWriteError(new Error("BOOM!")))
        }
      }

      testWith(versionedHybridStore)
    }
  }

  override def createIdent: String = randomAlphanumeric

  override def withVersionedStoreImpl[R](initialEntries: Entries, storeContext: MemoryHybridStoreWithMaxima[String, Record, Record])(testWith: TestWith[VersionedStoreImpl, R]): R = {

    initialEntries.map {
      case (k, v) => storeContext.put(k)(v).right.value
    }

    val versionedHybridStore = new MemoryVersionedHybridStore[String, Record, Record](storeContext)

    testWith(versionedHybridStore)
  }

  override def withVersionedStoreContext[R](testWith: TestWith[MemoryHybridStoreWithMaxima[String, Record, Record], R]): R = {
    val indexedStore = new MemoryStore[Version[String, Int], HybridIndexedStoreEntry[String, Record]](Map.empty)
      with MemoryMaxima[String, HybridIndexedStoreEntry[String, Record]]

    val memoryStoreForStreamStore = new MemoryStore[String, MemoryStreamStoreEntry](Map.empty)
    val streamStore = new MemoryStreamStore[String](memoryStoreForStreamStore)
    val typedStore = new MemoryTypedStore[String, Record](Map.empty)(streamStore, codec)

    testWith(new MemoryHybridStoreWithMaxima[String, Record, Record]()(typedStore, indexedStore, codec))
  }

  override def createT: HybridStoreEntry[Record, Record] = HybridStoreEntry(createRecord, createRecord)

  override def withStoreImpl[R](initialEntries: Map[Version[String, Int], HybridStoreEntry[Record, Record]], storeContext: MemoryHybridStoreWithMaxima[String, Record, Record])(testWith: TestWith[StoreImpl, R]): R =
    withVersionedStoreImpl(initialEntries, storeContext)(testWith)

  override def withStoreContext[R](testWith: TestWith[MemoryHybridStoreWithMaxima[String, Record, Record], R]): R =
    withVersionedStoreContext(testWith)

  override def withNamespace[R](testWith: TestWith[String, R]): R = testWith(randomAlphanumeric)

  override def createId(implicit namespace: String): Version[String, Int] = Version(randomAlphanumeric, 0)
}
