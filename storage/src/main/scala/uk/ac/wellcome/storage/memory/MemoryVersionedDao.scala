package uk.ac.wellcome.storage.memory

import uk.ac.wellcome.storage.VersionedDao
import uk.ac.wellcome.storage.type_classes.{VersionGetter, VersionUpdater}

class MemoryVersionedDao[T](
  val underlying: MemoryConditionalUpdateDao[T]
)(
  implicit
  val versionGetter: VersionGetter[T],
  val versionUpdater: VersionUpdater[T]
) extends VersionedDao[T]
