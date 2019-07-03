package uk.ac.wellcome.storage.store.memory

import uk.ac.wellcome.storage.store._
import uk.ac.wellcome.storage.streaming.Codec

class MemoryHybridStore[Ident, T, Metadata](
  implicit
  val typedStore: MemoryTypedStore[String, T],
  val indexedStore: MemoryStore[Ident,
                                HybridIndexedStoreEntry[String, Metadata]],
  val codec: Codec[T]
) extends HybridStore[Ident, String, T, Metadata] {

  override protected def createTypeStoreId(id: Ident): String = id.toString
}
