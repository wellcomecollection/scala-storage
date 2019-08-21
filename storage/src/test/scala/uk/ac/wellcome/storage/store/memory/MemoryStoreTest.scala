package uk.ac.wellcome.storage.store.memory

import uk.ac.wellcome.storage.generators.{Record, RecordGenerators}
import uk.ac.wellcome.storage.store.StoreWithOverwritesTestCases
import uk.ac.wellcome.storage.store.fixtures.StringNamespaceFixtures

class MemoryStoreTest
    extends StoreWithOverwritesTestCases[
      String,
      Record,
      String,
      MemoryStore[String, Record]]
    with MemoryStoreFixtures[String, Record, String]
    with StringNamespaceFixtures
    with RecordGenerators {

  override def createT: Record =
    createRecord
}
