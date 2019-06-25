package uk.ac.wellcome.storage.store.memory

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.store.TypedStoreEntry
import uk.ac.wellcome.storage.store.fixtures.TypedStoreFixtures
import uk.ac.wellcome.storage.streaming.Codec

trait MemoryTypedStoreFixtures[Ident, T] extends MemoryStreamStoreFixtures[Ident] with TypedStoreFixtures[Ident, T, MemoryStreamStore[Ident], MemoryTypedStore[Ident, T], MemoryStore[Ident, MemoryStreamStoreEntry]] {
  def withTypedStore[R](streamStore: MemoryStreamStore[Ident], initialEntries: Map[Ident, TypedStoreEntry[T]])(testWith: TestWith[MemoryTypedStore[Ident, T], R])(implicit codec: Codec[T]): R = {
    implicit val memoryStreamStore: MemoryStreamStore[Ident] = streamStore

    withMemoryTypedStoreImpl(initialEntries) { typedStore =>
      testWith(typedStore)
    }
  }

  def withMemoryTypedStoreImpl[R](initialEntries: Map[Ident, TypedStoreEntry[T]])(testWith: TestWith[MemoryTypedStore[Ident, T], R])(implicit streamStore: MemoryStreamStore[Ident], codec: Codec[T]): R =
    testWith(
      new MemoryTypedStore[Ident, T](initialEntries)
    )
}
