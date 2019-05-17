package uk.ac.wellcome.storage.memory

import uk.ac.wellcome.storage.ConditionalUpdateDao
import uk.ac.wellcome.storage.type_classes.VersionGetter

import scala.util.{Failure, Try}

class MemoryConditionalUpdateDao[Ident, T](
  underlying: MemoryDao[Ident, T]
)(
  implicit versionGetter: VersionGetter[T]
) extends ConditionalUpdateDao[Ident, T] {
  override def get(id: Ident): Try[Option[T]] = underlying.get(id)

  override def put(t: T): Try[T] = {
    val id = underlying.idGetter.id(t)

    val shouldUpdate = underlying.entries.get(id) match {
      case Some(existing) =>
        versionGetter.version(existing) < versionGetter.version(t)
      case None => true
    }

    if (shouldUpdate) {
      underlying.put(t)
    } else {
      Failure(new Throwable("Rejected! Version is going backwards."))
    }
  }
}

object MemoryConditionalUpdateDao {
  def apply[Ident, T](): MemoryConditionalUpdateDao[Ident, T] =
    new MemoryConditionalUpdateDao[Ident, T](
      new MemoryDao[Ident, T]()
    )
}
