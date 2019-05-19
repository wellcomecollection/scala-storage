package uk.ac.wellcome.storage.memory

import uk.ac.wellcome.storage.VersionedDao
import uk.ac.wellcome.storage.type_classes.{IdGetter, VersionGetter, VersionUpdater}

class MemoryVersionedDao[Ident, T](
  val underlying: MemoryConditionalUpdateDao[Ident, T]
)(
  implicit
  val versionGetter: VersionGetter[T],
  val versionUpdater: VersionUpdater[T]
) extends VersionedDao[Ident, T]

object MemoryVersionedDao {
  def apply[Ident, T]()(
    implicit
    idGetter: IdGetter[T],
    versionGetter: VersionGetter[T],
    versionUpdater: VersionUpdater[T]
  ): MemoryVersionedDao[Ident, T] =
    new MemoryVersionedDao(
      MemoryConditionalUpdateDao[Ident, T]()
    )
}
