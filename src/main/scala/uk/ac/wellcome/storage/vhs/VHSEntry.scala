package uk.ac.wellcome.storage.vhs

case class VHSEntry[M](
  hybridRecord: HybridRecord,
  metadata: M
)
