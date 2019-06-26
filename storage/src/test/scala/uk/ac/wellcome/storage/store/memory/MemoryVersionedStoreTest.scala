package uk.ac.wellcome.storage.store.memory

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.generators.{Record, RecordGenerators}
import uk.ac.wellcome.storage.maxima.memory.MemoryMaxima
import uk.ac.wellcome.storage.store.VersionedStoreTestCases

class MemoryVersionedStoreTest
  extends VersionedStoreTestCases[String, Record, MemoryStore[Version[String, Int], Record] with MemoryMaxima[String, Record]]
    with RecordGenerators {

  type UnderlyingMemoryStore = MemoryStore[Version[String, Int], Record] with MemoryMaxima[String, Record]

  override def createIdent: String = randomAlphanumeric
  override def createT: Record = createRecord

  override def withVersionedStoreImpl[R](initialEntries: Entries, storeContext: UnderlyingMemoryStore)(testWith: TestWith[VersionedStoreImpl, R]): R = {
    initialEntries.map {
      case (k,v) => storeContext.put(k)(v)
    }

    testWith(new MemoryVersionedStore(storeContext))
  }

  override def withVersionedStoreContext[R](testWith: TestWith[UnderlyingMemoryStore, R]): R =
    testWith(new MemoryStore[Version[String, Int], Record](Map.empty) with MemoryMaxima[String, Record])

  override def withFailingGetVersionedStore[R](initialEntries: Entries)(testWith: TestWith[VersionedStoreImpl, R]): R = {
    val store = new MemoryStore[Version[String, Int], Record](initialEntries) with MemoryMaxima[String, Record] {
      override def get(id: Version[String, Int]): Either[ReadError, Identified[Version[String, Int], Record]] = {
        Left(StoreReadError(new Error("BOOM!")))
      }
    }
    testWith(new MemoryVersionedStore(store))
  }

  override def withFailingPutVersionedStore[R](initialEntries: Entries)(testWith: TestWith[VersionedStoreImpl, R]): R = {
    val store = new MemoryStore[Version[String, Int], Record](initialEntries) with MemoryMaxima[String, Record] {
      override def put(id: Version[String, Int])(t: Record): Either[WriteError, Identified[Version[String, Int], Record]] = {
        Left(StoreWriteError(new Error("BOOM!")))
      }
    }
    testWith(new MemoryVersionedStore(store))
  }
}
