package uk.ac.wellcome.storage.memory

import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.type_classes.{IdGetter, VersionGetter}

class MemoryConditionalUpdateDao[Ident, T](
  underlying: MemoryDao[Ident, T]
)(
  implicit versionGetter: VersionGetter[T]
) extends ConditionalUpdateDao[Ident, T] {
  override def get(id: Ident): GetResult = underlying.get(id)

  override def put(t: T): PutResult = {
    val id = underlying.idGetter.id(t)

    val shouldUpdate = underlying.entries.get(id) match {
      case Some(existing) =>
        versionGetter.version(existing) < versionGetter.version(t)
      case None => true
    }

    if (shouldUpdate) {
      underlying.put(t)
    } else {
      Left(
        ConditionalWriteError(
          new Throwable("Rejected! Version is going backwards."))
      )
    }
  }
}

object MemoryConditionalUpdateDao {
  def apply[Ident, T]()(
    implicit
    idGetter: IdGetter[T],
    versionGetter: VersionGetter[T]
  ): MemoryConditionalUpdateDao[Ident, T] =
    new MemoryConditionalUpdateDao[Ident, T](
      new MemoryDao[Ident, T]()
    )
}
