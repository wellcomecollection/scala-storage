package uk.ac.wellcome.storage.store.memory

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.generators.{MetadataGenerators, Record, RecordGenerators}
import uk.ac.wellcome.storage.store.fixtures.StringNamespaceFixtures
import uk.ac.wellcome.storage.store.{TypedStoreEntry, TypedStoreTestCases}

class MemoryTypedStoreTest extends TypedStoreTestCases[String, Record, String, MemoryStreamStore[String], MemoryStore[String, MemoryStoreEntry]] with MemoryTypedStoreFixtures[String, Record] with MetadataGenerators with RecordGenerators with StringNamespaceFixtures {
  override def createT: TypedStoreEntry[Record] = TypedStoreEntry(createRecord, metadata = createValidMetadata)

  override def withBrokenStreamingStore[R](testWith: TestWith[MemoryStreamStore[String], R]): R = {
    val brokenMemoryStore = new MemoryStore[String, MemoryStoreEntry](initialEntries = Map.empty) {
      override def get(id: String): Either[ReadError, Identified[String, MemoryStoreEntry]] = Left(
        StoreReadError(new Throwable("get: BOOM!"))
      )

      override def put(id: String)(t: MemoryStoreEntry): Either[WriteError, Identified[String, MemoryStoreEntry]] = Left(
        StoreWriteError(
          new Throwable("put: BOOM!")
        )
      )
    }

    testWith(
      new MemoryStreamStore[String](brokenMemoryStore)
    )
  }
}
