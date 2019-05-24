package uk.ac.wellcome.storage.fixtures

import uk.ac.wellcome.storage.ObjectStore
import uk.ac.wellcome.storage.memory.MemoryObjectStore
import uk.ac.wellcome.storage.streaming.Codec

trait MemoryBuilders {
  def createObjectStore[T](implicit codec: Codec[T]): ObjectStore[T] =
    new MemoryObjectStore[T]()
}
