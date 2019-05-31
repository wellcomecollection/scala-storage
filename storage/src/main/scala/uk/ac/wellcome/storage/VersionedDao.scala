package uk.ac.wellcome.storage

import uk.ac.wellcome.storage.type_classes.{VersionGetter, VersionUpdater}

trait VersionedDao[Ident, T] {
  implicit val versionGetter: VersionGetter[T]
  implicit val versionUpdater: VersionUpdater[T]

  implicit val underlying: Dao[Ident, T]

  def get(id: Ident): Either[ReadError, T] = underlying.get(id)

  def put(value: T): Either[WriteError, T] = {
    val version = versionGetter.version(value)
    val newVersion = version + 1

    versionUpdater.updateVersion(value, newVersion = newVersion)
    val toStore = versionUpdater.updateVersion(value, newVersion = newVersion)
    underlying.put(toStore).map { _ =>
      toStore
    }
  }
}
