package uk.ac.wellcome.storage.memory

import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.type_classes.IdGetter

class MemoryDao[Ident, T](implicit val idGetter: IdGetter[T])
    extends Dao[Ident, T] {
  var entries: Map[String, T] = Map.empty

  override def get(id: Ident): GetResult =
    entries.get(id.toString) match {
      case Some(t) => Right(t)
      case None => Left(DoesNotExistError(
        new Throwable(s"No such entry: $id")
      ))
    }

  override def put(t: T): PutResult = {
    entries = entries + (idGetter.id(t) -> t)
    Right(())
  }
}
