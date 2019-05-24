package uk.ac.wellcome.storage.fixtures

import uk.ac.wellcome.storage.memory.{MemoryObjectStore, MemoryVersionedDao}
import uk.ac.wellcome.storage.streaming.Codec
import uk.ac.wellcome.storage.type_classes.{IdGetter, VersionGetter, VersionUpdater}
import uk.ac.wellcome.storage.vhs.{Entry, VersionedHybridStore}

trait MemoryBuilders {
  def createObjectStore[T](implicit codec: Codec[T]): MemoryObjectStore[T] =
    new MemoryObjectStore[T]()

  def createVersionedDao[T](
    implicit
    idGetter: IdGetter[T],
    versionGetter: VersionGetter[T],
    versionUpdater: VersionUpdater[T]
  ): MemoryVersionedDao[String, T] = MemoryVersionedDao[String, T]()

  def createVhs[T, Metadata](
    store: MemoryObjectStore[T] = createObjectStore[T],
    dao: MemoryVersionedDao[String, Entry[String, Metadata]] = createVersionedDao[Entry[String, Metadata]],
    testNamespace: String = "testing"
  ): VersionedHybridStore[String, T, Metadata] =
    new VersionedHybridStore[String, T, Metadata] {
      override protected val versionedDao: MemoryVersionedDao[String, Entry[String, Metadata]] = dao
      override protected val objectStore: MemoryObjectStore[T] = store
      override protected val namespace: String = testNamespace
    }
}
