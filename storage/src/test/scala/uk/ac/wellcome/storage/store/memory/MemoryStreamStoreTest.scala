package uk.ac.wellcome.storage.store.memory

import uk.ac.wellcome.storage.store.StreamStoreTestCases
import uk.ac.wellcome.storage.store.fixtures.StringNamespaceFixtures

class MemoryStreamStoreTest
  extends StreamStoreTestCases[String, String, MemoryStreamStore[String], MemoryStore[String, MemoryStoreEntry]]
    with MemoryStreamStoreFixtures[String]
    with StringNamespaceFixtures
