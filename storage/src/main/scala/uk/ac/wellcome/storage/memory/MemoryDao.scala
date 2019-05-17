package uk.ac.wellcome.storage.memory

import uk.ac.wellcome.storage.Dao
import uk.ac.wellcome.storage.type_classes.IdGetter

import scala.util.{Success, Try}

class MemoryDao[T](implicit val idGetter: IdGetter[T]) extends Dao[String, T] {
  protected var entries: Map[String, T] = Map.empty

  override def get(id: String): Try[Option[T]] = Success(entries.get(id))

  override def put(t: T): Try[T] = {
    entries = entries + (idGetter.id(t) -> t)
    Success(t)
  }
}
