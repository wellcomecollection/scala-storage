package uk.ac.wellcome.storage.store

import java.io.InputStream

import org.scalatest.{Assertion, FunSpec, Matchers}
import uk.ac.wellcome.storage.IncorrectStreamLengthError
import uk.ac.wellcome.storage.store.fixtures.ReplayableStreamFixtures
import uk.ac.wellcome.storage.streaming._

// TODO: Strictly speaking, a StreamingStore just cares about a vanilla InputStream,
// and we should put the `HasLength` and `HasMetadata` test cases into separate
// traits.  This starts to get awkward with the underlying StoreTestCases trait
// if you want them both, so I've left it for now.  Would be nice to fix another time.
//
trait StreamingStoreTestCases[Ident, IS <: InputStream with HasLength with HasMetadata, StoreContext]
  extends FunSpec
    with Matchers
    with StreamAssertions
    with ReplayableStreamFixtures
    with StoreTestCases[Ident, InputStream with HasLength with HasMetadata, String, StoreContext] {

  override def createT: ReplayableStream =
    createReplayableStream

  override def assertEqualT(original: InputStream with HasLength with HasMetadata, stored: InputStream with HasLength with HasMetadata): Assertion = {
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
