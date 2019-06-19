package uk.ac.wellcome.storage.transfer.memory

import uk.ac.wellcome.storage.generators.RandomThings
import uk.ac.wellcome.storage.store.memory.MemoryStore
import uk.ac.wellcome.storage.transfer.TransferTestCases

class MemoryTransferTest extends TransferTestCases[String, Array[Byte], MemoryStore[String, Array[Byte]], MemoryStore[String, Array[Byte]]] with MemoryTransferFixtures[String, Array[Byte]] with RandomThings {
  override def createSrcLocation(implicit context: MemoryStore[String, Array[Byte]]): String = randomAlphanumeric

  override def createDstLocation(implicit context: MemoryStore[String, Array[Byte]]): String = randomAlphanumeric

  override def createT: Array[Byte] = randomBytes()
}
