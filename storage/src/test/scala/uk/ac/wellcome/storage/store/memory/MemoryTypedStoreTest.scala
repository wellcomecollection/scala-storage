package uk.ac.wellcome.storage.store.memory

import java.io.InputStream

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.generators.{
  MetadataGenerators,
  Record,
  RecordGenerators
}
import uk.ac.wellcome.storage.store.fixtures.StringNamespaceFixtures
import uk.ac.wellcome.storage.store.{TypedStoreEntry, TypedStoreTestCases}
import uk.ac.wellcome.storage.streaming.InputStreamWithLengthAndMetadata

class MemoryTypedStoreTest
    extends TypedStoreTestCases[
      String,
      Record,
      String,
      MemoryStreamStore[String],
      MemoryTypedStore[String, Record],
      MemoryStore[String, MemoryStreamStoreEntry]]
    with MemoryTypedStoreFixtures[String, Record]
    with MetadataGenerators
    with RecordGenerators
    with StringNamespaceFixtures {
  override def createT: TypedStoreEntry[Record] =
    TypedStoreEntry(createRecord, metadata = createValidMetadata)

  override def withBrokenStreamStore[R](
    testWith: TestWith[MemoryStreamStore[String], R]): R = {
    val brokenMemoryStore = new MemoryStore[String, MemoryStreamStoreEntry](
      initialEntries = Map.empty) {
      override def get(id: String)
        : Either[ReadError, Identified[String, MemoryStreamStoreEntry]] = Left(
        StoreReadError(new Throwable("get: BOOM!"))
      )

      override def put(id: String)(t: MemoryStreamStoreEntry)
        : Either[WriteError, Identified[String, MemoryStreamStoreEntry]] =
        Left(
          StoreWriteError(
            new Throwable("put: BOOM!")
          )
        )
    }

    testWith(
      new MemoryStreamStore[String](brokenMemoryStore)
    )
  }

  override def withSingleValueStreamStore[R](rawStream: InputStream)(
    testWith: TestWith[MemoryStreamStore[String], R]): R = {
    val memoryStore = new MemoryStore[String, MemoryStreamStoreEntry](
      initialEntries = Map.empty)

    testWith(
      new MemoryStreamStore[String](memoryStore) {
        override def get(id: String): ReadEither = Right(
          Identified(
            id,
            new InputStreamWithLengthAndMetadata(
              rawStream,
              length = 0,
              metadata = Map.empty))
        )
      }
    )
  }

  it("can be created from a companion object") {
    val memoryTypedStore = MemoryTypedStore[String, Record]()
    memoryTypedStore shouldBe a[MemoryTypedStore[_, _]]
  }
}
