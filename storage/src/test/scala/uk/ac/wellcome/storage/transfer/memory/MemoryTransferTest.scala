package uk.ac.wellcome.storage.transfer.memory

import uk.ac.wellcome.storage.store.fixtures.StringNamespaceFixtures
import uk.ac.wellcome.storage.store.memory.MemoryStore
import uk.ac.wellcome.storage.transfer.TransferTestCases

class MemoryTransferTest
    extends TransferTestCases[
      String,
      Array[Byte],
      String,
      MemoryStore[String, Array[Byte]] with MemoryTransfer[String,
                                                           Array[Byte]]]
    with MemoryTransferFixtures[String, Array[Byte]]
    with StringNamespaceFixtures {
  override def createSrcLocation(implicit namespace: String): String = createId

  override def createDstLocation(implicit namespace: String): String = createId

  override def createT: Array[Byte] = randomBytes()
}
