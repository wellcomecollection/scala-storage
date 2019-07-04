package uk.ac.wellcome.storage.store.memory

import uk.ac.wellcome.storage.Version
import uk.ac.wellcome.storage.maxima.Maxima
import uk.ac.wellcome.storage.maxima.memory.MemoryMaxima
import uk.ac.wellcome.storage.store.VersionedStore

class MemoryVersionedStore[Id, T](
  store: MemoryStore[Version[Id, Int], T] with Maxima[Id, Int]
) extends VersionedStore[Id, Int, T](store)

object MemoryVersionedStore {
  def apply[Id, T](initialEntries: Map[Version[Id, Int], T]): MemoryVersionedStore[Id, T] =
    new MemoryVersionedStore[Id, T](
      store = new MemoryStore[Version[Id, Int], T](initialEntries)
      with MemoryMaxima[Id, T]
    )
}
