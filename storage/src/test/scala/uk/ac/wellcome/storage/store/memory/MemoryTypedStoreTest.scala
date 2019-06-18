package uk.ac.wellcome.storage.store.memory

import uk.ac.wellcome.storage.generators.{MetadataGenerators, Record, RecordGenerators}
import uk.ac.wellcome.storage.store.fixtures.StringNamespaceFixtures
import uk.ac.wellcome.storage.store.{TypedStoreEntry, TypedStoreTestCases}

class MemoryTypedStoreTest extends TypedStoreTestCases[String, Record, String, MemoryStore[String, MemoryStoreEntry]] with MemoryTypedStoreFixtures[String, Record] with MetadataGenerators with RecordGenerators with StringNamespaceFixtures {
  override def createT: TypedStoreEntry[Record] = TypedStoreEntry(createRecord, metadata = createValidMetadata)
}
