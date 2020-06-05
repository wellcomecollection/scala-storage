package uk.ac.wellcome.storage.tags.memory

import uk.ac.wellcome.storage.{DoesNotExistError, ReadError, WriteError}
import uk.ac.wellcome.storage.tags.Tags

class MemoryTags[Ident](initialTags: Map[Ident, Map[String, String]]) extends Tags[Ident] {
  private var underlying: Map[Ident, Map[String, String]] = initialTags

  override def get(id: Ident): Either[ReadError, Map[String, String]] =
    underlying.get(id) match {
      case Some(tags) => Right(tags)
      case None       => Left(DoesNotExistError())
    }

  override def put(id: Ident, tags: Map[String, String]): Either[WriteError, Map[String, String]] = {
    underlying = underlying ++ Map(id -> tags)
    Right(tags)
  }
}
