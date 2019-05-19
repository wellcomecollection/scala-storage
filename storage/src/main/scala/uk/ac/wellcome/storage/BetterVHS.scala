package uk.ac.wellcome.storage

import grizzled.slf4j.Logging

import scala.util.{Success, Try}

case class BetterVHSEntry[Ident, Metadata](
  id: Ident,
  version: Int,
  location: ObjectLocation,
  metadata: Metadata
)

trait BetterVHS[Ident, T, Metadata] extends Logging {
  type VHSEntry = BetterVHSEntry[Ident, Metadata]

  protected val namespace: String = ""

  protected val versionedDao: VersionedDao[Ident, VHSEntry]
  protected val objectStore: ObjectStore[T]

  def update(
    id: Ident
  )(
    ifNotExisting: => (T, Metadata)
  )(
    ifExisting: (T, Metadata) => (T, Metadata)
  ): Try[VHSEntry] =
    getObject(id).flatMap {
      case Some((storedObject, storedRow)) =>
        debug(s"Found existing object for $id")
        val (newObject, newMetadata) =
          ifExisting(storedObject, storedRow.metadata)

        if (newObject != storedObject || newMetadata != storedRow.metadata) {
          debug(s"Object for $id changed, updating")
          putObject(
            id = id,
            existingRow = storedRow,
            newObject = newObject,
            newMetadata = newMetadata
          )
        } else {
          debug(s"Existing object for $id unchanged, not updating")
          Success(storedRow)
        }
      case None =>
        debug(s"There's no existing object for $id")
        val (newObject, newMetadata) = ifNotExisting
        putNewObject(id = id, newObject, newMetadata)
    }

  def get(id: Ident): Try[Option[T]] =
    getObject(id).map {
      case Some((t, _)) => Some(t)
      case None         => None
    }

  private def getObject(id: Ident): Try[Option[(T, VHSEntry)]] =
    versionedDao.get(id).flatMap {
      case Some(row) =>
        objectStore.get(row.location).map { t: T =>
          Some((t, row))
        }
      case None => Success(None)
    }

  private def putObject(
    id: Ident,
    existingRow: VHSEntry,
    newObject: T,
    newMetadata: Metadata): Try[VHSEntry] =
    for {
      newLocation <- objectStore.put(namespace)(
        newObject,
        keyPrefix = KeyPrefix(id.toString)
      )
      newRow <- versionedDao.put(
        existingRow.copy(
          location = newLocation,
          metadata = newMetadata
        )
      )
    } yield newRow

  private def putNewObject(id: Ident, t: T, metadata: Metadata): Try[VHSEntry] =
    for {
      location <- objectStore.put(namespace)(
        t,
        keyPrefix = KeyPrefix(id.toString)
      )
      row <- versionedDao.put(
        BetterVHSEntry[Ident, Metadata](
          id = id,
          version = 0,
          location = location,
          metadata = metadata
        )
      )
    } yield row
}
