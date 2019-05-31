package uk.ac.wellcome.storage.memory

import uk.ac.wellcome.storage.streaming.Codec
import uk.ac.wellcome.storage.ObjectStore

class MemoryObjectStore[T](
  implicit val codec: Codec[T]
) extends ObjectStore[T] {
  override implicit val storageBackend: MemoryStorageBackend =
    new MemoryStorageBackend()
}
