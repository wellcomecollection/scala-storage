package uk.ac.wellcome.storage.store.memory

import uk.ac.wellcome.storage.store.{HybridStoreEntry, VersionedStore}

class MemoryVersionedHybridStore[Id, T, Metadata](
  store: MemoryHybridStoreWithMaxima[Id, T, Metadata]
) extends VersionedStore[Id, Int, HybridStoreEntry[T, Metadata]](store)
