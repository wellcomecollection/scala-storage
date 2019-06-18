package uk.ac.wellcome.storage.store.memory

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.store.fixtures.TypedStoreFixtures
import uk.ac.wellcome.storage.store.{TypedStore, TypedStoreEntry}

trait MemoryTypedStoreFixtures[Ident, T] extends MemoryStreamStoreFixtures[Ident] with TypedStoreFixtures[Ident, T, MemoryStore[String, MemoryStoreEntry]] {
  def withTypedStoreImpl[R](storeContext: MemoryStore[Ident, MemoryStoreEntry], initialEntries: Map[Ident, TypedStoreEntry[T]])(testWith: TestWith[TypedStore[Ident, T], R]): R = {
    withMemoryStreamStoreImpl(storeContext, initialEntries = Map.empty) { implicit streamStore =>
      testWith(
        new MemoryTypedStore[Ident, T](initialEntries)
      )
    }
  }
}
