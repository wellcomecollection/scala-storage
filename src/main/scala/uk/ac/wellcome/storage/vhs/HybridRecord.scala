package uk.ac.wellcome.storage.vhs

import uk.ac.wellcome.storage.ObjectLocation

case class HybridRecord(
  id: String,
  version: Int,
  location: ObjectLocation
)
