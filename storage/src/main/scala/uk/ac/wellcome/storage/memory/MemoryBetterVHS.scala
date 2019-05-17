package uk.ac.wellcome.storage.memory

import uk.ac.wellcome.storage.{BetterVHS, BetterVHSEntry}

class MemoryBetterVHS[Ident, T, Metadata](
  val versionedDao: MemoryVersionedDao[Ident, BetterVHSEntry[Ident, Metadata]],
  val objectStore: MemoryObjectStore[T]
) extends BetterVHS[Ident, T, Metadata]
