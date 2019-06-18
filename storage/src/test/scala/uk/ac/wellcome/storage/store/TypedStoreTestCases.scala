package uk.ac.wellcome.storage.store

import uk.ac.wellcome.fixtures.TestWith
import uk.ac.wellcome.storage.StoreReadError
import uk.ac.wellcome.storage.store.fixtures.TypedStoreFixtures
import uk.ac.wellcome.storage.streaming.InputStreamWithLengthAndMetadata

trait TypedStoreTestCases[Ident, T, Namespace, StreamStoreImpl <: StreamStore[Ident, InputStreamWithLengthAndMetadata], StreamStoreContext]
  extends StoreTestCases[Ident, TypedStoreEntry[T], Namespace, StreamStoreContext]
  with TypedStoreFixtures[Ident, T, StreamStoreImpl, StreamStoreContext] {

  override def withStoreImpl[R](storeContext: StreamStoreContext, initialEntries: Map[Ident, TypedStoreEntry[T]])(testWith: TestWith[StoreImpl, R]): R =
    withTypedStoreImpl(storeContext, initialEntries) { typedStore =>
      testWith(typedStore)
    }

  override def withStoreContext[R](testWith: TestWith[StreamStoreContext, R]): R =
    withStreamStoreContext { context =>
      testWith(context)
    }

  def withBrokenStreamingStore[R](testWith: TestWith[StreamStoreImpl, R]): R

  describe("behaves as a TypedStore") {
    describe("get") {
      it("errors if the streaming store has an error") {
        withNamespace { implicit identContext =>
          withBrokenStreamingStore { brokenStreamStore =>
            withTypedStore(brokenStreamStore, initialEntries = Map.empty) { typedStore =>
              val result = typedStore.get(createId).left.value

              result shouldBe a[StoreReadError]
            }
          }
        }
      }
    }
  }
//
//      it("if we're getting the raw stream, we don't close it") {
//        withNamespace { implicit identContext =>
//          val rawStream = new CloseDetectionStream(randomUTF16String)
//          withCustomInputStream(rawStream) { streamingStore =>
//            withInputStreamStoreImpl { store =>
//              store.get(createId)
//
//              rawStream.isClosed shouldBe false
//            }(streamingStore)
//          }
//        }
//      }
//
//      it("if we're interpreting the stream, we close the raw stream") {
//        withNamespace { implicit identContext =>
//          val rawStream = new CloseDetectionStream(randomUTF16String)
//          withCustomInputStream(rawStream) { streamingStore =>
//
//            withStoreImpl(
//              storeContext = ObjectStoreContext(streamingStore, codec),
//              initialEntries = Map.empty) { store =>
//              store.get(createId)
//
//              rawStream.isClosed shouldBe true
//            }
//          }
//        }
//      }
//
//      it("if we're interpreting the stream and there's a decoding error, we still close the raw stream") {
//        withNamespace { implicit identContext =>
//          val rawStream = new CloseDetectionStream(randomUTF16String)
//          withCustomInputStream(rawStream) { streamingStore =>
//
//            withStoreImpl(
//              storeContext = ObjectStoreContext(streamingStore, codec),
//              initialEntries = Map.empty) { store =>
//              store.get(createId)
//
//              rawStream.isClosed shouldBe true
//            }
//          }
//        }
//      }
//
//      it("errors if it can't close the stream") {
//        withNamespace { implicit identContext =>
//          val string = createJsonT
//
//          val exception = new Throwable("BOOM!")
//
//          val rawStream = new CloseDetectionStream(string)
//
//          val closeShieldStream =
//            new FilterInputStream(rawStream) {
//              override def close(): Unit = throw exception
//            }
//
//          withNamespace { implicit identContext =>
//            withCustomInputStream(closeShieldStream) { streamingStore =>
//              withStoreImpl(
//                storeContext = ObjectStoreContext(streamingStore, codec),
//                initialEntries = Map.empty
//              ) { store =>
//                val result = store.get(createId).left.value
//                result shouldBe a[CannotCloseStreamError]
//                result.e shouldBe exception
//              }
//            }
//          }
//        }
//      }
//    }
//
//    describe("put") {
//      it("errors if the streaming store has an error") {
//        withNamespace { implicit identContext =>
//          withBrokenStreamingStore { implicit brokenStreamingStore =>
//
//            val storeContext =
//              ObjectStoreContext(brokenStreamingStore, codec)
//
//            withStoreImpl(
//              storeContext = storeContext,
//              initialEntries = Map.empty
//            ) { store =>
//              val result = store.put(createId)(createT).left.value
//
//              result shouldBe a[BackendWriteError]
//            }
//          }
//        }
//      }
//
//      it("errors if the data in the streaming store is the wrong format") {
//        withNamespace { implicit identContext =>
//          val id = createId
//
//          val entry = StreamingStoreEntry(
//            stream = stringCodec.toStream("Not a JSON string").right.value,
//            metadata = Map.empty
//          )
//
//          withStreamingStore() { streamingStore =>
//
//            streamingStore.put(id)(entry)
//
//            val context = ObjectStoreContext(streamingStore, codec)
//
//            withStoreImpl(context, Map.empty) { store =>
//              val result = store.get(id).left.value
//
//              result shouldBe a[DecoderError]
//            }
//          }
//        }
//      }
//
//      it("errors if the codec can't create a stream") {
//        withStoreContext { storeContext =>
//          withNamespace { implicit identContext =>
//            val exception = new Throwable("BOOM!")
//
//            implicit val brokenCodec: Codec[T] = new Codec[T] {
//              override def toStream(t: T): Either[EncoderError, FiniteInputStream] =
//                Left(JsonEncodingError(exception))
//
//              override def fromStream(inputStream: FiniteInputStream): Either[DecoderError, T] =
//                Left(JsonDecodingError(exception))
//            }
//
//            withStoreImpl(
//              storeContext = storeContext.copy(codec = brokenCodec),
//              initialEntries = Map.empty
//            ) { store =>
//              val result = store.put(createId)(createT).left.value
//
//              result shouldBe a[JsonEncodingError]
//              result.e shouldBe exception
//            }
//          }
//        }
//      }
//    }
//  }

}

//import java.io.{FilterInputStream, InputStream}
//
//import uk.ac.wellcome.fixtures.TestWith
//import uk.ac.wellcome.storage._
//import uk.ac.wellcome.storage.generators.RandomThings
//import uk.ac.wellcome.storage.streaming.Codec.stringCodec
//import uk.ac.wellcome.storage.streaming.{Codec, FiniteInputStream}
//
//
//
//case class ObjectStoreContext[Context, T](context: Context, codec: Codec[T])
//
//trait ObjectStoreTestCases[T, IdentContext, StreamingStoreImpl <: StreamingStore]
//  extends StoreTestCases[ObjectLocation, ObjectStoreEntry[T], IdentContext, ObjectStoreContext[StreamingStoreImpl, T]]
//    with RandomThings {
////
////  implicit val codec: Codec[T]
////  type ObjectStoreImpl <: ObjectStore[T]
////
////  def withStreamingStore[R](initialEntries: Map[ObjectLocation, ObjectStoreEntry[T]] = Map.empty)(testWith: TestWith[StreamingStoreImpl, R]): R
////
////  def withBrokenStreamingStore[R](testWith: TestWith[StreamingStoreImpl, R]): R
////
////  def withObjectStore[R](streamingStore: StreamingStoreImpl)(testWith: TestWith[ObjectStoreImpl, R])(implicit codec: Codec[T]): R
////
////  override def withStoreImpl[R](storeContext: ObjectStoreContext[StreamingStoreImpl, T], initialEntries: Map[ObjectLocation, ObjectStoreEntry[T]])(testWith: TestWith[StoreImpl, R]): R =
////    withObjectStore(storeContext.context) { store =>
////      testWith(store)
////    }(storeContext.codec)
////
////  override def withStoreImpl[R](initialEntries: Map[ObjectLocation, ObjectStoreEntry[T]] = Map.empty)(testWith: TestWith[StoreImpl, R]): R =
////    withStreamingStore(initialEntries) { implicit streamingStore =>
////      withObjectStore(streamingStore) { store =>
////        testWith(store)
////      }
////    }
////
////  def createJsonT: String =
////    stringCodec.fromStream(codec.toStream(createT.objectStoreT).right.value).right.value
////
////  def withInputStreamStoreImpl[R](testWith: TestWith[ObjectStore[FiniteInputStream], R])(implicit streamingStore: StreamingStoreImpl): R
////
////  class CloseDetectionStream(string: String) extends FilterInputStream(stringCodec.toStream(string).right.value) {
////    var isClosed = false
////
////    override def close(): Unit = {
////      isClosed = true
////      super.close()
////    }
////  }
////
////  def withCustomInputStream[R](rawStream: InputStream)(testWith: TestWith[StreamingStoreImpl, R]): R
////
//}