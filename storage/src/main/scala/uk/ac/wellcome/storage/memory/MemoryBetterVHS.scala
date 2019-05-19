package uk.ac.wellcome.storage.memory

import uk.ac.wellcome.storage.vhs.{Entry, VersionedHybridStore}

class MemoryBetterVHS[Ident, T, Metadata](
  val versionedDao: MemoryVersionedDao[Ident, Entry[Ident, Metadata]],
  val objectStore: MemoryObjectStore[T]
) extends VersionedHybridStore[Ident, T, Metadata]
