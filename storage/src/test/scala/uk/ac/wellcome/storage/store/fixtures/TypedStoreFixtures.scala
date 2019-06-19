package uk.ac.wellcome.storage.store.fixtures

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.store.{StreamStore, TypedStore, TypedStoreEntry}
import uk.ac.wellcome.storage.streaming.{Codec, InputStreamWithLengthAndMetadata}

trait TypedStoreFixtures[Ident, T, StreamStoreImpl <: StreamStore[Ident, InputStreamWithLengthAndMetadata], TypedStoreImpl <: TypedStore[Ident, T], StreamStoreContext] extends StreamStoreFixtures[Ident, StreamStoreImpl, StreamStoreContext] {
  implicit val codec: Codec[T]

  def withTypedStore[R](streamStore: StreamStoreImpl, initialEntries: Map[Ident, TypedStoreEntry[T]])(testWith: TestWith[TypedStoreImpl, R])(implicit codec: Codec[T]): R

  def withTypedStoreImpl[R](storeContext: StreamStoreContext, initialEntries: Map[Ident, TypedStoreEntry[T]])(testWith: TestWith[TypedStoreImpl, R])(implicit codec: Codec[T]): R =
    withStreamStoreImpl(storeContext, initialEntries = Map.empty) { streamStore =>
      withTypedStore(streamStore, initialEntries) { typedStore =>
        testWith(typedStore)
      }
    }
}
