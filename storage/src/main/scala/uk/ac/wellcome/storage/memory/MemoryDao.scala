package uk.ac.wellcome.storage.memory

import uk.ac.wellcome.storage.Dao
import uk.ac.wellcome.storage.type_classes.IdGetter

import scala.util.{Success, Try}

class MemoryDao[Ident, T](implicit val idGetter: IdGetter[T]) extends Dao[Ident, T] {
  var entries: Map[String, T] = Map.empty

  override def get(id: Ident): Try[Option[T]] = Success(entries.get(id.toString))

  override def put(t: T): Try[T] = {
    entries = entries + (idGetter.id(t) -> t)
    Success(t)
  }
}
