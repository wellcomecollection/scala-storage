package uk.ac.wellcome.storage.memory

import uk.ac.wellcome.storage.{ObjectStore, StorageBackend}
import uk.ac.wellcome.storage.type_classes.SerialisationStrategy

class MemoryObjectStore[T](
  implicit val serialisationStrategy: SerialisationStrategy[T]
) extends ObjectStore[T] {
  override implicit val storageBackend: StorageBackend = new MemoryStorageBackend()
}
