package uk.ac.wellcome.storage.memory

import uk.ac.wellcome.storage.ConditionalUpdateDao
import uk.ac.wellcome.storage.type_classes.VersionGetter

import scala.util.{Failure, Try}

class MemoryConditionalUpdateDao[T](
  underlying: MemoryDao[T]
)(
  implicit versionGetter: VersionGetter[T]
) extends ConditionalUpdateDao[String, T] {
  override def get(id: String): Try[Option[T]] = underlying.get(id)

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
