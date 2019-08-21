package uk.ac.wellcome.storage.store.memory

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.store.fixtures.StreamStoreFixtures
import uk.ac.wellcome.storage.streaming.Codec.bytesCodec
import uk.ac.wellcome.storage.streaming.InputStreamWithLengthAndMetadata

trait MemoryStreamStoreFixtures[Ident]
    extends StreamStoreFixtures[
      Ident,
      MemoryStreamStore[Ident],
      MemoryStore[Ident, MemoryStreamStoreEntry]] {
  def withMemoryStreamStoreImpl[R](
    underlying: MemoryStore[Ident, MemoryStreamStoreEntry],
    initialEntries: Map[Ident, InputStreamWithLengthAndMetadata])(
    testWith: TestWith[MemoryStreamStore[Ident], R]): R = {
    val memoryStoreEntries =
      initialEntries.map {
        case (id, inputStream) =>
          (
            id,
            MemoryStreamStoreEntry(
              bytes = bytesCodec.fromStream(inputStream).right.value,
              metadata = inputStream.metadata))
      }

    underlying.entries = underlying.entries ++ memoryStoreEntries

    testWith(
      new MemoryStreamStore[Ident](underlying)
    )
  }

  override def withStreamStoreImpl[R](
    storeContext: MemoryStore[Ident, MemoryStreamStoreEntry],
    initialEntries: Map[Ident, InputStreamWithLengthAndMetadata])(
    testWith: TestWith[MemoryStreamStore[Ident], R]): R =
    withMemoryStreamStoreImpl(storeContext, initialEntries) { streamStore =>
      testWith(streamStore)
    }

  override def withStreamStoreContext[R](
    testWith: TestWith[MemoryStore[Ident, MemoryStreamStoreEntry], R]): R =
    testWith(
      new MemoryStore[Ident, MemoryStreamStoreEntry](
        initialEntries = Map.empty)
    )
}
