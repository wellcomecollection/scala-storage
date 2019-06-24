package uk.ac.wellcome.storage.store.memory

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.generators.{Record, RecordGenerators}
import uk.ac.wellcome.storage.maxima.memory.MemoryMaxima
import uk.ac.wellcome.storage.store.VersionedStoreTestCases

class MemoryVersionedStoreTest
  extends VersionedStoreTestCases[String, Record]
    with RecordGenerators {

  override def createIdent: String = randomAlphanumeric
  override def createT: Record = createRecord

  override def withVersionedStore[R](initialEntries: Entries)(testWith: TestWith[VersionedStoreImpl, R]): R = {
    val store = new MemoryStore[Version[String, Int], Record](initialEntries) with MemoryMaxima[String, Record]
    testWith(new MemoryVersionedStore(store))
  }

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
