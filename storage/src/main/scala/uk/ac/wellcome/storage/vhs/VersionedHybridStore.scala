package uk.ac.wellcome.storage.vhs

import grizzled.slf4j.Logging
import uk.ac.wellcome.storage.{KeyPrefix, ObjectStore, VersionedDao}

import scala.util.{Failure, Success, Try}

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
            storedObject = storedObject,
            storedRow = storedRow,
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

  private def getObject(id: Ident): Try[Option[(T, VHSEntry)]] = {
    val maybeRow = versionedDao.get(id)

    maybeRow match {
      case Success(Some(row)) =>
        objectStore
          .get(row.location)
          .map { t: T =>
            Some((t, row))
          }
          .recover {
            case t: Throwable =>
              throw new RuntimeException(
                s"Dao entry for $id points to a location that can't be fetched from the object store: $t"
              )
          }
      case Success(None) => Success(None)
      case Failure(err) =>
        Failure(
          new RuntimeException(s"Cannot read record $id from dao: $err")
        )
    }
  }

  private def putObject(id: Ident,
                        storedObject: T,
                        storedRow: VHSEntry,
                        newObject: T,
                        newMetadata: Metadata): Try[VHSEntry] =
    for {
      location <- if (storedObject != newObject) {
        objectStore.put(namespace)(
          newObject,
          keyPrefix = KeyPrefix(id.toString)
        )
      } else {
        Success(storedRow.location)
      }
      newRow <- versionedDao.put(
        storedRow.copy(
          location = location,
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
        Entry[Ident, Metadata](
          id = id,
          version = 0,
          location = location,
          metadata = metadata
        )
      )
    } yield row
}
