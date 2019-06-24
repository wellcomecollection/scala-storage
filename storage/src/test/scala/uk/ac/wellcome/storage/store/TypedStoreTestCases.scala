package uk.ac.wellcome.storage.store

import java.io.{FilterInputStream, InputStream}

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.generators.RandomThings
import uk.ac.wellcome.storage.store.fixtures.TypedStoreFixtures
import uk.ac.wellcome.storage.streaming.Codec._
import uk.ac.wellcome.storage.streaming.{Codec, InputStreamWithLength, InputStreamWithLengthAndMetadata}

trait TypedStoreTestCases[Ident, T, Namespace, StreamStoreImpl <: StreamStore[Ident, InputStreamWithLengthAndMetadata], TypedStoreImpl <: TypedStore[Ident, T], StreamStoreContext]
  extends StoreWithOverwritesTestCases[Ident, TypedStoreEntry[T], Namespace, StreamStoreContext]
  with TypedStoreFixtures[Ident, T, StreamStoreImpl, TypedStoreImpl, StreamStoreContext]
  with RandomThings {

  override def withStoreImpl[R](storeContext: StreamStoreContext, initialEntries: Map[Ident, TypedStoreEntry[T]])(testWith: TestWith[StoreImpl, R]): R =
    withTypedStoreImpl(storeContext, initialEntries) { typedStore =>
      testWith(typedStore)
    }

  override def withStoreContext[R](testWith: TestWith[StreamStoreContext, R]): R =
    withStreamStoreContext { context =>
      testWith(context)
    }

  def withBrokenStreamStore[R](testWith: TestWith[StreamStoreImpl, R]): R

  class CloseDetectionStream(bytes: Array[Byte]) extends FilterInputStream(bytesCodec.toStream(bytes).right.value) {
    var isClosed = false

    override def close(): Unit = {
      isClosed = true
      super.close()
    }
  }

  def withSingleValueStreamStore[R](rawStream: InputStream)(testWith: TestWith[StreamStoreImpl, R]): R

  describe("behaves as a TypedStore") {
    describe("get") {
      it("errors if the streaming store has an error") {
        withNamespace { implicit namespace =>
          withBrokenStreamStore { brokenStreamStore =>
            withTypedStore(brokenStreamStore, initialEntries = Map.empty) { typedStore =>
              val result = typedStore.get(createId).left.value

              result shouldBe a[StoreReadError]
            }
          }
        }
      }

      it("always closes the underlying stream") {
        withNamespace { implicit namespace =>
          val closeDetectionStream = new CloseDetectionStream(randomBytes())

          withSingleValueStreamStore(closeDetectionStream) { streamingStore =>
            withTypedStore(streamingStore, initialEntries = Map.empty) { typedStore =>
              typedStore.get(createId)

              closeDetectionStream.isClosed shouldBe true
            }
          }
        }
      }

      it("errors if it can't close the stream") {
        withNamespace { implicit namespace =>
          val exception = new Throwable("BOOM!")

          val rawStream = new CloseDetectionStream(randomBytes())

          val closeShieldStream =
            new FilterInputStream(rawStream) {
              override def close(): Unit = throw exception
            }

          withSingleValueStreamStore(closeShieldStream) { streamingStore =>
            withTypedStore(streamingStore, initialEntries = Map.empty) { typedStore =>
              val result = typedStore.get(createId).left.value
              result shouldBe a[CannotCloseStreamError]
              result.e shouldBe exception
            }
          }
        }
      }
    }
  }

  describe("put") {
    it("errors if the stream store has an error") {
      withNamespace { implicit namespace =>
        withBrokenStreamStore { implicit brokenStreamStore =>
          withTypedStore(brokenStreamStore, initialEntries = Map.empty) { typedStore =>
            val result = typedStore.put(createId)(createT).left.value

            result shouldBe a[StoreWriteError]
          }
        }
      }
    }

    it("errors if the data in the stream store is the wrong format") {
      withNamespace { implicit namespace =>
        val stream = stringCodec.toStream("Not a JSON string").right.value

        withSingleValueStreamStore(stream) { streamStore =>
          withTypedStore(streamStore, initialEntries = Map.empty) { typedStore =>
            val result = typedStore.get(createId).left.value

            result shouldBe a[DecoderError]
          }
        }
      }
    }

    it("errors if the codec can't create a stream") {
      withStoreContext { storeContext =>
        withNamespace { implicit namespace =>
          val exception = new Throwable("BOOM!")

          implicit val brokenCodec: Codec[T] = new Codec[T] {
            override def toStream(t: T): Either[EncoderError, InputStreamWithLength] =
              Left(JsonEncodingError(exception))

            override def fromStream(inputStream: InputStream): Either[DecoderError, T] =
              Left(JsonDecodingError(exception))
          }

          withTypedStoreImpl(storeContext, initialEntries = Map.empty) { typedStore =>
            val result = typedStore.put(createId)(createT).left.value

            result shouldBe a[JsonEncodingError]
          } (brokenCodec)
        }
      }
    }
  }
}
