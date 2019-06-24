package uk.ac.wellcome.storage.store.fixtures

import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.maxima.Maxima
import uk.ac.wellcome.storage.store.{Store, VersionedStore}

trait VersionedStoreFixtures[Id, T] {
  type StoreImpl = Store[Version[Id, Int], T] with Maxima[Id, Int]
  type VersionedStoreImpl = VersionedStore[Id, Int, T]
}

