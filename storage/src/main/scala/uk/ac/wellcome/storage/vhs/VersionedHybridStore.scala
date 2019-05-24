package uk.ac.wellcome.storage.vhs

import grizzled.slf4j.Logging
import uk.ac.wellcome.storage._

trait VersionedHybridStore[Ident, T, Metadata] extends Logging {
  type VHSEntry = Entry[Ident, Metadata]

  protected val namespace: String = ""

  protected val versionedDao: VersionedDao[Ident, VHSEntry]
  protected val objectStore: ObjectStore[T]

  def update(
    id: Ident
  )(
    ifNotExisting: => (T, Metadata)
  )(
    ifExisting: (T, Metadata) => (T, Metadata)
  ): Either[StorageError, VHSEntry] =
    getObject(id) match {
      case Right((storedObject, storedRow)) =>
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
          Right(storedRow)
        }

      case Left(DoesNotExistError(_)) =>
        debug(s"There's no existing object for $id")
        val (newObject, newMetadata) = ifNotExisting
        putNewObject(id = id, newObject, newMetadata)

      case Left(err) => Left(err)
    }

  def get(id: Ident): Either[ReadError, T] =
    getObject(id).map { case (t, _) => t }

  private def getObject(id: Ident): Either[ReadError, (T, VHSEntry)] =
    for {
      storedRow <- versionedDao.get(id)
      storedObject <- objectStore.get(storedRow.location)
    } yield (storedObject, storedRow)

  private def putObject(id: Ident,
                        existingRow: VHSEntry,
                        newObject: T,
                        newMetadata: Metadata): Either[WriteError, VHSEntry] =
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

  private def putNewObject(id: Ident,
                           t: T,
                           metadata: Metadata): Either[WriteError, VHSEntry] =
    for {
      location <- objectStore.put(namespace)(
        t,
        keyPrefix = KeyPrefix(id.toString)
      )
      _ = debug(s"New location = $location")
      row <- versionedDao.put(
        Entry[Ident, Metadata](
          id = id,
          version = 0,
          location = location,
          metadata = metadata
        )
      )
    } yield row
}
