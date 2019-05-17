package uk.ac.wellcome.storage

import uk.ac.wellcome.storage.type_classes.{VersionGetter, VersionUpdater}

import scala.util.Try

trait VersionedDao[T] {
  implicit val versionGetter: VersionGetter[T]
  implicit val versionUpdater: VersionUpdater[T]

  implicit val underlying: ConditionalUpdateDao[String, T]

  def get(id: String): Try[Option[T]] = underlying.get(id)

  def put(value: T): Try[T] = {
    val version = versionGetter.version(value)
    val newVersion = version + 1

    versionUpdater.updateVersion(value, newVersion = newVersion)
    val toStore = versionUpdater.updateVersion(value, newVersion = newVersion)
    underlying.put(toStore)
  }
}
