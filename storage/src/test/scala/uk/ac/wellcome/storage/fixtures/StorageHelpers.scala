package uk.ac.wellcome.storage.fixtures

import uk.ac.wellcome.storage.{ConditionalUpdateDao, Dao, VersionedDao}
import uk.ac.wellcome.storage.memory.{MemoryConditionalUpdateDao, MemoryDao, MemoryVersionedDao}

trait StorageHelpers {
  def createDao[Ident, T]: Dao[Ident, T] =
    new MemoryDao[Ident, T]

  def createConditionalUpdateDao[Ident, T]: ConditionalUpdateDao[Ident, T] =
    new MemoryConditionalUpdateDao(
      createDao[Ident, T].asInstanceOf[MemoryDao[Ident, T]]
    )

  def createVersionedDao[Ident, T]: VersionedDao[Ident, T] =
    new MemoryVersionedDao(
      createConditionalUpdateDao[Ident, T].asInstanceOf[MemoryConditionalUpdateDao[Ident, T]]
    )
}
