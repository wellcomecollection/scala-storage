package uk.ac.wellcome.storage.vhs

import uk.ac.wellcome.storage.ObjectLocation

case class Entry[Ident, Metadata](
  id: Ident,
  version: Int,
  location: ObjectLocation,
  metadata: Metadata
)

case class PlainEntry[Ident](
  id: Ident,
  version: Int,
  location: ObjectLocation
)
