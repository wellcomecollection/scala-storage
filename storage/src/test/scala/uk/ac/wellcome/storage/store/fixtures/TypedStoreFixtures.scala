package uk.ac.wellcome.storage.store.fixtures

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.store.{StreamStore, TypedStore, TypedStoreEntry}
import uk.ac.wellcome.storage.streaming.{Codec, InputStreamWithLengthAndMetadata}

trait TypedStoreFixtures[Ident, T, StreamStoreImpl <: StreamStore[Ident, InputStreamWithLengthAndMetadata], StreamStoreContext] extends StreamStoreFixtures[Ident, StreamStoreImpl, StreamStoreContext] {
  implicit val codec: Codec[T]

  def withTypedStoreImpl[R](streamStore: StreamStoreImpl, initialEntries: Map[Ident, TypedStoreEntry[T]])(testWith: TestWith[TypedStore[Ident, T], R]): R

  def withTypedStoreImpl[R](storeContext: StreamStoreContext, initialEntries: Map[Ident, TypedStoreEntry[T]])(testWith: TestWith[TypedStore[Ident, T], R]): R =
    withStreamStoreImpl(storeContext, initialEntries = Map.empty) { streamStore =>
      withTypedStoreImpl(streamStore, initialEntries) { typedStore =>
        testWith(typedStore)
      }
    }
}
