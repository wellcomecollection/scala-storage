package uk.ac.wellcome.storage.store

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.IncorrectStreamLengthError
import uk.ac.wellcome.storage.store.fixtures.{ReplayableStreamFixtures, StreamStoreFixtures}
import uk.ac.wellcome.storage.streaming._

trait StreamStoreTestCases[
  Ident,
  Namespace,
  StreamStoreImpl <: StreamStore[Ident],
  StreamStoreContext]
    extends AnyFunSpec
    with Matchers
    with StreamAssertions
    with ReplayableStreamFixtures
    with StreamStoreFixtures[Ident, StreamStoreImpl, StreamStoreContext]
    with StoreWithOverwritesTestCases[
      Ident,
      InputStreamWithLength,
      Namespace,
      StreamStoreContext] {

  override def withStoreImpl[R](
    initialEntries: Map[Ident, InputStreamWithLength],
    storeContext: StreamStoreContext)(testWith: TestWith[StoreImpl, R]): R =
    withStreamStoreImpl(storeContext, initialEntries) { streamStore =>
      testWith(streamStore)
    }

  override def withStoreContext[R](
    testWith: TestWith[StreamStoreContext, R]): R =
    withStreamStoreContext { context =>
      testWith(context)
    }

  override def createT: ReplayableStream =
    createReplayableStream

  override def assertEqualT(original: InputStreamWithLength, stored: InputStreamWithLength): Assertion = {
    val originalBytes = original.asInstanceOf[ReplayableStream].originalBytes
    assertStreamEquals(
      stored,
      originalBytes,
      expectedLength = originalBytes.length
    )
  }

  describe("it behaves as a StreamStore") {
    describe("get") {
      it("can get a stream without metadata") {
        withNamespace { implicit namespace =>
          val id = createId
          val initialEntry = ReplayableStream(randomBytes())

          withStoreImpl(initialEntries = Map(id -> initialEntry)) { store =>
            val retrievedEntry = store.get(id).right.value

            assertEqualT(initialEntry, retrievedEntry.identifiedT)
          }
        }
      }

      it("can get a stream with metadata") {
        withNamespace { implicit namespace =>
          val id = createId
          val initialEntry =
            ReplayableStream(randomBytes())

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
          val entry = ReplayableStream(randomBytes())

          withStoreImpl(initialEntries = Map.empty) { store =>
            store.put(id)(entry) shouldBe a[Right[_, _]]
          }
        }
      }

      it("can put a stream with metadata") {
        withNamespace { implicit namespace =>
          val id = createId
          val entry = ReplayableStream(randomBytes())

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
            length = bytes.length + 1
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
            length = bytes.length - 1
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
