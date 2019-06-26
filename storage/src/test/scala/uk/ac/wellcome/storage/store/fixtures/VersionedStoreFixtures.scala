package uk.ac.wellcome.storage.store.fixtures

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.maxima.Maxima
import uk.ac.wellcome.storage.store.{Store, VersionedStore}

trait VersionedStoreFixtures[Id, V, T, VersionedStoreContext] {
  type StoreImpl = Store[Version[Id, V], T] with Maxima[Id, V]
  type VersionedStoreImpl = VersionedStore[Id, V, T]

  type Entries = Map[Version[Id, V], T]

  def createIdent: Id
  def createT: T

  def withVersionedStoreImpl[R](initialEntries: Entries, storeContext: VersionedStoreContext)(testWith: TestWith[VersionedStoreImpl, R]): R
  def withVersionedStoreContext[R](testWith: TestWith[VersionedStoreContext, R]): R

  def withVersionedStoreImpl[R](initialEntries: Entries = Map.empty)(testWith: TestWith[VersionedStoreImpl, R]): R =
    withVersionedStoreContext { storeContext =>
      withVersionedStoreImpl(initialEntries, storeContext) { versionedStoreImpl =>
        testWith(versionedStoreImpl)
      }
    }
}

