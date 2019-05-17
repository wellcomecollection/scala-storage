package uk.ac.wellcome.storage.memory

import uk.ac.wellcome.storage.type_classes.{IdGetter, VersionGetter, VersionUpdater}
import uk.ac.wellcome.storage.{BetterVersionedDao, Dao}

import scala.util.{Failure, Try}

class MemoryVersionedDao[T](
  implicit
  idGetter: IdGetter[T],
  val versionGetter: VersionGetter[T],
  val versionUpdater: VersionUpdater[T]
) extends BetterVersionedDao[T] {
  override implicit val underlying: Dao[String, T] = new MemoryDao[T]() {
    override def put(t: T): Try[T] = {
      val id = idGetter.id(t)

      val shouldUpdate = entries.get(id) match {
        case Some(existing) => versionGetter.version(existing) < versionGetter.version(t)
        case None => true
      }

      if (shouldUpdate) {
        super.put(t)
      } else {
        Failure(new Throwable("Rejected! Version is going backwards."))
      }
    }
  }
}
