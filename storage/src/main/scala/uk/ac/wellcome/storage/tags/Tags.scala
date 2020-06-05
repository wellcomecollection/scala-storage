package uk.ac.wellcome.storage.tags

import grizzled.slf4j.Logging
import uk.ac.wellcome.storage._

trait Tags[Ident] extends Logging {
  def get(id: Ident): Either[ReadError, Map[String, String]]

  protected def put(
    id: Ident,
    tags: Map[String, String]): Either[WriteError, Map[String, String]]

  def update(id: Ident)(
    updateFunction: Map[String, String] => Either[UpdateFunctionError,
                                                  Map[String, String]])
    : Either[UpdateError, Map[String, String]] = {
    info(s"Tags on $id: updating tags")

    for {
      existingTags <- get(id) match {
        case Right(value)                 => Right(value)
        case Left(err: DoesNotExistError) => Left(UpdateNoSourceError(err))
        case Left(err)                    => Left(UpdateReadError(err))
      }

      _ = debug(s"Tags on $id: existing tags = $existingTags")

      newTags <- updateFunction(existingTags)

      _ = debug(s"Tags on $id: new tags      = $newTags")

      result <- if (newTags == existingTags) {
        debug(s"Tags on $id: no change, so skipping a write")
        Right(existingTags)
      } else {
        debug(s"Tags on $id: tags have changed, so writing new tags")
        put(id = id, tags = newTags) match {
          case Right(value) => Right(value)
          case Left(err)    => Left(UpdateWriteError(err))
        }
      }
    } yield result
  }
}
