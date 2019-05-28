package uk.ac.wellcome.storage.memory

import uk.ac.wellcome.storage.streaming.Codec
import uk.ac.wellcome.storage.{ObjectStore, StorageBackend}

class MemoryObjectStore[T](
  implicit val codec: Codec[T]
) extends ObjectStore[T] {
  override implicit val storageBackend: StorageBackend =
    new MemoryStorageBackend()
}
