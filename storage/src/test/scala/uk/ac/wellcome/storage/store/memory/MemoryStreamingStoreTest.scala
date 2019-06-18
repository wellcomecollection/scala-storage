package uk.ac.wellcome.storage.store.memory

import java.io.InputStream

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.generators.MetadataGenerators
import uk.ac.wellcome.storage.store.StreamingStoreTestCases
import uk.ac.wellcome.storage.store.fixtures.StringNamespaceFixtures
import uk.ac.wellcome.storage.streaming.Codec.bytesCodec
import uk.ac.wellcome.storage.streaming.{InputStreamWithLengthAndMetadata, HasLength, HasMetadata}

class MemoryStreamingStoreTest extends StreamingStoreTestCases[String, InputStreamWithLengthAndMetadata, MemoryStore[String, MemoryStoreEntry]] with MetadataGenerators with StringNamespaceFixtures {
  override def withStoreImpl[R](storeContext: MemoryStore[String, MemoryStoreEntry], initialEntries: Map[String, InputStream with HasLength with HasMetadata])(testWith: TestWith[StoreImpl, R]): R = {
    val memoryStoreEntries =
      initialEntries.map { case (id, inputStream) =>
        (id, MemoryStoreEntry(bytes = bytesCodec.fromStream(inputStream).right.value, metadata = inputStream.metadata))
      }

    storeContext.entries = storeContext.entries ++ memoryStoreEntries

    testWith(
      new MemoryStreamingStore[String](storeContext)
    )
  }

  override def withStoreContext[R](testWith: TestWith[MemoryStore[String, MemoryStoreEntry], R]): R =
    testWith(
      new MemoryStore[String, MemoryStoreEntry](initialEntries = Map.empty)
    )
}
