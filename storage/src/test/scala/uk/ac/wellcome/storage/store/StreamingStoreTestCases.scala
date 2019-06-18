package uk.ac.wellcome.storage.store

import java.io.InputStream

import org.scalatest.{Assertion, FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.IncorrectStreamLengthError
import uk.ac.wellcome.storage.generators.MetadataGenerators
import uk.ac.wellcome.storage.store.fixtures.StringNamespaceFixtures
import uk.ac.wellcome.storage.store.memory.{MemoryStore, MemoryStoreEntry, MemoryStreamingStore}
import uk.ac.wellcome.storage.streaming.Codec._
import uk.ac.wellcome.storage.streaming._

class MemoryStreamingStoreTest extends StreamingStoreWithMetadataTestCases[String, FiniteInputStreamWithMetadata, MemoryStore[String, MemoryStoreEntry]] with MetadataGenerators with StringNamespaceFixtures {
  override def withStoreImpl[R](storeContext: MemoryStore[String, MemoryStoreEntry], initialEntries: Map[String, InputStream with FiniteStream with HasMetadata])(testWith: TestWith[StoreImpl, R]): R = {
    val memoryStoreEntries =
      initialEntries.map { case (id, inputStream) =>
        (id, MemoryStoreEntry(bytes = bytesCodec.fromStream(inputStream).right.value, metadata = inputStream.metadata))
      }

    storeContext.entries = storeContext.entries ++ memoryStoreEntries

    testWith(
      new MemoryStreamingStore[String](storeContext)
    )
  }

  override def withStoreContext[R](testWith: TestWith[MemoryStore[String, MemoryStoreEntry], R]): R =
    testWith(
      new MemoryStore[String, MemoryStoreEntry](initialEntries = Map.empty)
    )
}

trait StreamingStoreWithMetadataTestCases[Ident, IS <: InputStream with FiniteStream with HasMetadata, StoreContext]
  extends FunSpec
    with Matchers
    with StreamAssertions
    with MetadataGenerators
    with StoreTestCases[Ident, InputStream with FiniteStream with HasMetadata, String, StoreContext] {

  class ReplayableStream(val originalBytes: Array[Byte], length: Long, metadata: Map[String, String]) extends FiniteInputStreamWithMetadata(
    inputStream = bytesCodec.toStream(originalBytes).right.value,
    length = length,
    metadata = metadata
  )

  object ReplayableStream {
    def apply(bytes: Array[Byte], metadata: Map[String, String]): ReplayableStream =
      new ReplayableStream(bytes, length = bytes.length, metadata = metadata)
  }

  def createReplayableStream: ReplayableStream =
    ReplayableStream(randomBytes(), metadata = createValidMetadata)

  override def createT: ReplayableStream =
    createReplayableStream

  override def assertEqualT(original: InputStream with FiniteStream with HasMetadata, stored: InputStream with FiniteStream with HasMetadata): Assertion = {
    original.metadata shouldBe stored.metadata

    val originalBytes = original.asInstanceOf[ReplayableStream].originalBytes
    assertStreamEquals(stored, originalBytes, expectedLength = originalBytes.length)
  }

  describe("it behaves as a streaming store") {
    describe("get") {
      it("can get a stream without metadata") {
        withNamespace { implicit namespace =>
          val id = createId
          val initialEntry = ReplayableStream(randomBytes(), metadata = Map.empty)

          withStoreImpl(initialEntries = Map(id -> initialEntry)) { store =>
            val retrievedEntry = store.get(id).right.value

            assertEqualT(initialEntry, retrievedEntry.identifiedT)
          }
        }
      }

      it("can get a stream with metadata") {
        withNamespace { implicit namespace =>
          val id = createId
          val initialEntry = ReplayableStream(randomBytes(), metadata = createValidMetadata)

          withStoreImpl(initialEntries = Map(id -> initialEntry)) { store =>
            val retrievedEntry = store.get(id).right.value

            assertEqualT(initialEntry, retrievedEntry.identifiedT)
          }
        }
      }
    }

    describe("put") {
      it("can put a stream without metadata") {
        withNamespace { implicit namespace =>
          val id = createId
          val entry = ReplayableStream(randomBytes(), metadata = Map.empty)

          withStoreImpl(initialEntries = Map.empty) { store =>
            store.put(id)(entry) shouldBe a[Right[_, _]]
          }
        }
      }

      it("can put a stream with metadata") {
        withNamespace { implicit namespace =>
          val id = createId
          val entry = ReplayableStream(randomBytes(), metadata = createValidMetadata)

          withStoreImpl(initialEntries = Map.empty) { store =>
            store.put(id)(entry) shouldBe a[Right[_, _]]
          }
        }
      }

      it("errors if the stream length is too long") {
        withNamespace { implicit namespace =>
          val bytes = randomBytes()
          val brokenStream = new ReplayableStream(
            bytes,
            length = bytes.length + 1,
            metadata = createValidMetadata
          )

          withStoreImpl(initialEntries = Map.empty) { store =>
            val result = store.put(createId)(brokenStream).left.value

            result shouldBe a[IncorrectStreamLengthError]
          }
        }
      }

      it("errors if the stream length is too short") {
        withNamespace { implicit namespace =>
          val bytes = randomBytes()
          val brokenStream = new ReplayableStream(
            bytes,
            length = bytes.length - 1,
            metadata = createValidMetadata
          )

          withStoreImpl(initialEntries = Map.empty) { store =>
            val result = store.put(createId)(brokenStream).left.value

            result shouldBe a[IncorrectStreamLengthError]
          }
        }
      }
    }
  }
}
