package uk.ac.wellcome.storage.store.fixtures

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.store.{TypedStore, TypedStoreEntry}
import uk.ac.wellcome.storage.streaming.Codec

trait TypedStoreFixtures[Ident, T, StreamStoreContext] {
  implicit val codec: Codec[T]

  def withTypedStoreImpl[R](storeContext: StreamStoreContext, initialEntries: Map[Ident, TypedStoreEntry[T]])(testWith: TestWith[TypedStore[Ident, T], R]): R
}
