package uk.ac.wellcome.storage.memory

import uk.ac.wellcome.storage.VersionedDao
import uk.ac.wellcome.storage.type_classes.{VersionGetter, VersionUpdater}

class MemoryVersionedDao[Ident, T](
  val underlying: MemoryConditionalUpdateDao[Ident, T]
)(
  implicit
  val versionGetter: VersionGetter[T],
  val versionUpdater: VersionUpdater[T]
) extends VersionedDao[Ident, T]
