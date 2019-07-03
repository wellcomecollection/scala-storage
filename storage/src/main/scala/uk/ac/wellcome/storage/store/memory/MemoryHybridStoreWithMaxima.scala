package uk.ac.wellcome.storage.store.memory

import uk.ac.wellcome.storage.Version
import uk.ac.wellcome.storage.maxima.Maxima
import uk.ac.wellcome.storage.store.{
  HybridIndexedStoreEntry,
  HybridStoreWithMaxima,
  Store
}
import uk.ac.wellcome.storage.streaming.Codec

class MemoryHybridStoreWithMaxima[Id, T, Metadata](
  implicit
  val typedStore: MemoryTypedStore[String, T],
  val indexedStore: Store[
    Version[Id, Int],
    HybridIndexedStoreEntry[String, Metadata]] with Maxima[Id, Int],
  val codec: Codec[T]
) extends HybridStoreWithMaxima[Id, Int, String, T, Metadata] {

  override protected def createTypeStoreId(id: Version[Id, Int]): String =
    id.toString
}
