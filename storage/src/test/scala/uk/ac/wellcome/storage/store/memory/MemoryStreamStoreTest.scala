package uk.ac.wellcome.storage.store.memory

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.store.StreamStoreTestCases
import uk.ac.wellcome.storage.store.fixtures.StringNamespaceFixtures
import uk.ac.wellcome.storage.streaming.Codec.bytesCodec
import uk.ac.wellcome.storage.streaming.InputStreamWithLengthAndMetadata

class MemoryStreamStoreTest extends StreamStoreTestCases[String, String, MemoryStore[String, MemoryStoreEntry]] with StringNamespaceFixtures {
  override def withStoreImpl[R](storeContext: MemoryStore[String, MemoryStoreEntry], initialEntries: Map[String, InputStreamWithLengthAndMetadata])(testWith: TestWith[StoreImpl, R]): R = {
    val memoryStoreEntries =
      initialEntries.map { case (id, inputStream) =>
        (id, MemoryStoreEntry(bytes = bytesCodec.fromStream(inputStream).right.value, metadata = inputStream.metadata))
      }

    storeContext.entries = storeContext.entries ++ memoryStoreEntries

    testWith(
      new MemoryStreamStore[String](storeContext)
    )
  }

  override def withStoreContext[R](testWith: TestWith[MemoryStore[String, MemoryStoreEntry], R]): R =
    testWith(
      new MemoryStore[String, MemoryStoreEntry](initialEntries = Map.empty)
    )
}
