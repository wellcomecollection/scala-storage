package uk.ac.wellcome.storage.memory

import uk.ac.wellcome.storage.{
  ObjectStore,
  SerialisationStrategy,
  StorageBackend
}

class MemoryObjectStore[T](
  implicit val serialisationStrategy: SerialisationStrategy[T]
) extends ObjectStore[T] {
  override implicit val storageBackend: StorageBackend =
    new MemoryStorageBackend()
}
