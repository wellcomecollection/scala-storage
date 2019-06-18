package uk.ac.wellcome.storage.store

import java.io.InputStream

import org.scalatest.{Assertion, EitherValues, FunSpec, Matchers}
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.generators.{MetadataGenerators, RandomThings}
import uk.ac.wellcome.storage.store.memory.{MemoryStore, MemoryStoreEntry, MemoryStreamingStore}
import uk.ac.wellcome.storage.streaming.Codec._
import uk.ac.wellcome.storage.streaming._

trait StringNamespaceFixtures extends RandomThings {
  def withNamespace[R](testWith: TestWith[String, R]): R =
    testWith(randomAlphanumeric)

  def createId(implicit namespace: String): String =
    s"$namespace/$randomAlphanumeric"
}

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
    with EitherValues
    with StreamAssertions
    with MetadataGenerators
    with StoreTestCases[Ident, InputStream with FiniteStream with HasMetadata, String, StoreContext] {

  // TODO: This should use bytes, not String

  class ReplayableStream(val originalString: String, metadata: Map[String, String]) extends FiniteInputStreamWithMetadata(
    inputStream = stringCodec.toStream(originalString).right.get,
    length = originalString.getBytes.length,
    metadata = metadata
  )

  override def createT: ReplayableStream =
    new ReplayableStream(randomAlphanumeric, metadata = createValidMetadata)

  override def assertEqualT(original: InputStream with FiniteStream with HasMetadata, stored: InputStream with FiniteStream with HasMetadata): Assertion = {
    original.metadata shouldBe stored.metadata

    assertStreamEquals(stored, original.asInstanceOf[ReplayableStream].originalString)
  }

  describe("it behaves as a streaming store") {

  }


//
//  override def createId(implicit Context: String): ObjectLocation =
//    createObjectLocationWith(namespace = Context)
//
//  def withStoreNamespace[R](testWith: TestWith[String, R]): R = withNamespace { Context =>
//    testWith(Context)
//  }
//

//

//
//  def createStoreEntryWith(randomString: String = Random.nextString(8), metadata: Map[String, String] = createValidMetadata): StreamingStoreEntry =
//    StreamingStoreEntry(
//      stream = new ReplayableFiniteInputStream(randomString),
//      metadata = metadata
//    )
//
//  def createStoreEntry: StreamingStoreEntry = createStoreEntryWith()
//
//  def createT: StreamingStoreEntry = createStoreEntry
//
//  describe("StreamingStore") {
//    describe("get") {
//      it("can get a stream without metadata") {
//        withStoreNamespace { namespace =>
//          val location = createObjectLocationWith(namespace)
//          val initialEntry = createStoreEntryWith(metadata = Map.empty)
//
//          withStoreImpl(initialEntries = Map(location -> initialEntry)) { store =>
//            val retrievedEntry = store.get(location).right.value
//
//            assertEqualT(initialEntry, retrievedEntry.identifiedT)
//          }
//        }
//      }
//
//      it("can get a stream with metadata") {
//        withStoreNamespace { namespace =>
//          val location = createObjectLocationWith(namespace)
//          val initialEntry = createStoreEntryWith(metadata = createValidMetadata)
//
//          withStoreImpl(initialEntries = Map(location -> initialEntry)) { store =>
//            val retrievedEntry = store.get(location).right.value
//
//            assertEqualT(initialEntry, retrievedEntry.identifiedT)
//          }
//        }
//      }
//    }
//
//    describe("put") {
//      it("can put a stream without metadata") {
//        withStoreNamespace { namespace =>
//          val location = createObjectLocationWith(namespace)
//          val entry = createStoreEntryWith(metadata = Map.empty)
//
//          withStoreImpl() { store =>
//            store.put(location)(entry) shouldBe a[Right[_, _]]
//          }
//        }
//      }
//
//      it("can put a stream with metadata") {
//        withStoreNamespace { namespace =>
//          val location = createObjectLocationWith(namespace)
//          val entry = createStoreEntryWith(metadata = createValidMetadata)
//
//          withStoreImpl() { store =>
//            store.put(location)(entry) shouldBe a[Right[_, _]]
//          }
//        }
//      }
//
//      it("can overwrite an existing stream") {
//        withStoreNamespace { namespace =>
//          val location = createObjectLocationWith(namespace)
//          val entry1 = createStoreEntry
//          val entry2 = createStoreEntry
//
//          withStoreImpl() { store =>
//            store.put(location)(entry1) shouldBe a[Right[_, _]]
//            store.put(location)(entry2) shouldBe a[Right[_, _]]
//          }
//        }
//      }
//
//      it("errors if the stream length is too long") {
//        withStoreNamespace { namespace =>
//          val entry = createStoreEntry
//          val brokenEntry = entry.copy(
//            stream = new FiniteInputStream(
//              entry.stream,
//              length = entry.stream.length + 10
//            )
//          )
//
//          withStoreImpl() { store =>
//            val result = store.put(createObjectLocationWith(namespace))(brokenEntry).left.value
//
//            result shouldBe a[IncorrectStreamLengthError]
//          }
//        }
//      }
//
//      it("errors if the stream length is too short") {
//        withStoreNamespace { namespace =>
//          val entry = createStoreEntry
//          val brokenEntry = entry.copy(
//            stream = new FiniteInputStream(
//              entry.stream,
//              length = entry.stream.length - 10
//            )
//          )
//
//          withStoreImpl() { store =>
//            val result = store.put(createObjectLocationWith(namespace))(brokenEntry).left.value
//
//            result shouldBe a[IncorrectStreamLengthError]
//          }
//        }
//      }
//    }
//  }
}
