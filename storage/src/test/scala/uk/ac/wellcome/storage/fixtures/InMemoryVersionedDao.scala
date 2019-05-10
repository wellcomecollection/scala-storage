package uk.ac.wellcome.storage.fixtures

import uk.ac.wellcome.storage.VersionedDao
import uk.ac.wellcome.storage.type_classes.{IdGetter, VersionGetter, VersionUpdater}

import scala.util.{Failure, Success, Try}

class InMemoryVersionedDao[T](
  implicit
  idGetter: IdGetter[T],
  versionGetter: VersionGetter[T],
  versionUpdater: VersionUpdater[T]
) extends VersionedDao[T] {
  var storage: Map[String, T] = Map.empty

  override def get(id: String): Try[Option[T]] =
    Success(
      storage.get(id)
    )

  override def put(value: T): Try[T] = {
    val id = idGetter.id(value)
    val valueToStore = versionUpdater.updateVersion(
      value, newVersion = versionGetter.version(value) + 1
    )

    val storeNewVersion = storage.get(id) match {
      case None => true
      case Some(existingValue) =>
        versionGetter.version(existingValue) < versionGetter.version(value)
    }

    if (storeNewVersion) {
      storage = storage ++ Map(id -> valueToStore)
      Success(valueToStore)
    } else {
      Failure(new Throwable(s"Already exists with a new version! ${storage.get(id)}"))
    }
  }
}
