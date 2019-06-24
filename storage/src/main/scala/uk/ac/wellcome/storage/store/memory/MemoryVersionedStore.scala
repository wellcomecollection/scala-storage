package uk.ac.wellcome.storage.store.memory

import uk.ac.wellcome.storage.Version
import uk.ac.wellcome.storage.maxima.Maxima
import uk.ac.wellcome.storage.store.VersionedStore

class MemoryVersionedStore[Id, V, T](
  store: MemoryStore[Version[Id, V], T] with Maxima[Id, V]
)(implicit N: Numeric[V])
    extends VersionedStore[Id, V, T](store)
