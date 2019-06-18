package uk.ac.wellcome.storage.store.memory

import org.apache.commons.io.IOUtils
import uk.ac.wellcome.storage.store._
import uk.ac.wellcome.storage.streaming.Codec

class MemoryTypedStore[Ident, T](
  initialEntries: Map[Ident, TypedStoreEntry[T]] =
    Map.empty[Ident, TypedStoreEntry[T]])(
  implicit
  val streamStore: MemoryStreamStore[Ident],
  val codec: Codec[T]
) extends TypedStore[Ident, T] {

  val initial = initialEntries.map {
    case (location, entry) =>
      location -> MemoryStoreEntry(
        IOUtils.toByteArray(
          codec.toStream(entry.t).right.get
        ),
        entry.metadata
      )
  }

  streamStore.memoryStore.entries =
    streamStore.memoryStore.entries ++ initial
}
