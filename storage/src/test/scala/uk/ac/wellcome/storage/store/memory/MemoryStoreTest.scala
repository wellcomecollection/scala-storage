package uk.ac.wellcome.storage.store.memory

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.generators.{Record, RecordGenerators}
import uk.ac.wellcome.storage.store.StoreTestCases

class MemoryStoreTest
  extends StoreTestCases[String, Record, String, MemoryStore[String, Record]]
    with MemoryStoreFixtures[String, Record, String]
    with RecordGenerators {

  override def withNamespace[R](testWith: TestWith[String, R]): R =
    testWith(randomAlphanumeric)

  override def createId(implicit namespace: String): String =
    s"$namespace/$randomAlphanumeric"

  override def createT: Record =
    createRecord
}
