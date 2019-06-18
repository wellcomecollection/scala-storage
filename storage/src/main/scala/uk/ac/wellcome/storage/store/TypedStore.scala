package uk.ac.wellcome.storage.store

import uk.ac.wellcome.storage.streaming.{Codec, InputStreamWithLengthAndMetadata}
import uk.ac.wellcome.storage._

import scala.util.{Failure, Success, Try}

case class TypedStoreEntry[T](t: T, metadata: Map[String, String])

trait TypedStore[Ident, T] extends Store[Ident, TypedStoreEntry[T]] {
  implicit val codec: Codec[T]
  implicit val streamStore: StreamStore[Ident, InputStreamWithLengthAndMetadata]

  override def get(id: Ident): ReadEither =
    for {
      internalEntry <- streamStore.get(id)
      decodeResult <- codec.fromStream(internalEntry.identifiedT) match {
        case value => Right(value)
      }
      t <- ensureStreamClosed(internalEntry, decodeResult)
      entry = TypedStoreEntry(t, internalEntry.identifiedT.metadata)
    } yield Identified(id, entry)

  private def ensureStreamClosed(entry: Identified[Ident, InputStreamWithLengthAndMetadata], decodeResult: Either[DecoderError, T]): Either[ReadError, T] = {
    val underlying = entry.identifiedT

    // We need to ensure the underlying stream is closed, unless we're
    // returning that raw -- in which case we shouldn't!
    decodeResult match {
      case Right(is) if is == underlying => decodeResult
      case _ =>
        Try(underlying.close()) match {
          case Success(_)   => decodeResult
          case Failure(err) => Left(CannotCloseStreamError(err))
        }
    }
  }

  override def put(id: Ident)(entry: TypedStoreEntry[T]): WriteEither = for {
    stream <- codec.toStream(entry.t)
    _ <- streamStore.put(id)(
      InputStreamWithLengthAndMetadata(
        stream, metadata = entry.metadata
      )
    )
  } yield Identified(id, entry)
}

