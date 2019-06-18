package uk.ac.wellcome.storage.store.memory

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.store.fixtures.StreamStoreFixtures
import uk.ac.wellcome.storage.streaming.Codec.bytesCodec
import uk.ac.wellcome.storage.streaming.InputStreamWithLengthAndMetadata

trait MemoryStreamStoreFixtures[Ident] extends StreamStoreFixtures[Ident, MemoryStore[Ident, MemoryStoreEntry]] {
  override def withStreamStoreImpl[R](storeContext: MemoryStore[Ident, MemoryStoreEntry], initialEntries: Map[Ident, InputStreamWithLengthAndMetadata])(testWith: TestWith[StreamStore[Ident, InputStreamWithLengthAndMetadata], R]): R = {
    val memoryStoreEntries =
      initialEntries.map { case (id, inputStream) =>
        (id, MemoryStoreEntry(bytes = bytesCodec.fromStream(inputStream).right.value, metadata = inputStream.metadata))
      }

    storeContext.entries = storeContext.entries ++ memoryStoreEntries

    testWith(
      new MemoryStreamStore[Ident](storeContext)
    )
  }

  override def withStreamStoreContext[R](testWith: TestWith[MemoryStore[Ident, MemoryStoreEntry], R]): R =
    testWith(
      new MemoryStore[Ident, MemoryStoreEntry](initialEntries = Map.empty)
    )
}
