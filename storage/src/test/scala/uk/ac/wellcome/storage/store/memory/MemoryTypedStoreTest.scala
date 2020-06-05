package uk.ac.wellcome.storage.store.memory

import java.io.InputStream

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.generators.{MetadataGenerators, Record, RecordGenerators}
import uk.ac.wellcome.storage.store.TypedStoreTestCases
import uk.ac.wellcome.storage.store.fixtures.StringNamespaceFixtures
import uk.ac.wellcome.storage.streaming.InputStreamWithLength

class MemoryTypedStoreTest
    extends TypedStoreTestCases[
      String,
      Record,
      String,
      MemoryStreamStore[String],
      MemoryTypedStore[String, Record],
      MemoryStore[String, Array[Byte]]]
    with MemoryTypedStoreFixtures[String, Record]
    with MetadataGenerators
    with RecordGenerators
    with StringNamespaceFixtures {
  override def createT: Record = createRecord

  override def withBrokenStreamStore[R](
    testWith: TestWith[MemoryStreamStore[String], R]): R = {
    val brokenMemoryStore = new MemoryStore[String, Array[Byte]](
      initialEntries = Map.empty) {
      override def get(id: String)
        : Either[ReadError, Identified[String, Array[Byte]]] = Left(
        StoreReadError(new Throwable("get: BOOM!"))
      )

      override def put(id: String)(bytes: Array[Byte])
        : Either[WriteError, Identified[String, Array[Byte]]] =
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
    val memoryStore = new MemoryStore[String, Array[Byte]](
      initialEntries = Map.empty)

    testWith(
      new MemoryStreamStore[String](memoryStore) {
        override def get(id: String): ReadEither = Right(
          Identified(
            id,
            new InputStreamWithLength(rawStream, length = 0)
          )
        )
      }
    )
  }

  it("can be created from a companion object") {
    val memoryTypedStore = MemoryTypedStore[String, Record]()
    memoryTypedStore shouldBe a[MemoryTypedStore[_, _]]
  }
}
