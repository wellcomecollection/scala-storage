package uk.ac.wellcome.storage.store.memory

import uk.ac.wellcome.storage.ReadError
import uk.ac.wellcome.storage.maxima.Maxima
import uk.ac.wellcome.storage.maxima.memory.MemoryMaxima
import uk.ac.wellcome.storage.store._
import uk.ac.wellcome.storage.streaming.Codec


class MemoryHybridStore[Ident, T, Metadata](
                                      implicit
                                      val typedStore: MemoryTypedStore[String, T],
                                      val indexedStore: MemoryStore[Ident, HybridIndexedStoreEntry[Ident, String, Metadata]]
                                        with MemoryMaxima[Ident, HybridIndexedStoreEntry[Ident, String, Metadata]],
                                      val codec: Codec[T]
                                    ) extends HybridStore[Ident, String, T, Metadata] with Maxima[Ident, Int] {

  override protected def createTypeStoreId(id: Ident): String = id.toString
  override def max(q: Ident): Either[ReadError, Int] = indexedStore.max(q)
}
