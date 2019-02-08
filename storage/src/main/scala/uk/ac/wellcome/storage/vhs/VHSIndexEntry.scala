package uk.ac.wellcome.storage.vhs

case class VHSIndexEntry[M](
  hybridRecord: HybridRecord,
  metadata: M
)
