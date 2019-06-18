package uk.ac.wellcome.storage.store.memory

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.generators.RandomThings
import uk.ac.wellcome.storage.store.fixtures.StoreFixtures

trait MemoryStoreFixtures[Ident, T, Namespace]
  extends StoreFixtures[Ident, T, Namespace, MemoryStore[Ident, T]]
  with RandomThings {

  override def withStoreImpl[R](storeContext: MemoryStore[Ident, T], initialEntries: Map[Ident, T])(testWith: TestWith[StoreImpl, R]): R = {
    storeContext.entries = storeContext.entries ++ initialEntries

    testWith(storeContext)
  }

  def withStoreContext[R](testWith: TestWith[MemoryStore[Ident, T], R]): R =
    testWith(
      new MemoryStore[Ident, T](initialEntries = Map.empty)
    )
}
