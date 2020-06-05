package uk.ac.wellcome.storage.store

import uk.ac.wellcome.storage.streaming.{Codec, InputStreamWithLength}
import uk.ac.wellcome.storage._

import scala.util.{Failure, Success, Try}

trait TypedStore[Ident, T] extends Store[Ident, T] {
  implicit val codec: Codec[T]
  implicit val streamStore: StreamStore[Ident]

  override def get(id: Ident): ReadEither =
    for {
      internalEntry <- streamStore.get(id)
      decodeResult <- codec.fromStream(internalEntry.identifiedT) match {
        case value => Right(value)
      }
      t <- ensureStreamClosed(internalEntry, decodeResult)
    } yield Identified(id, t)

  private def ensureStreamClosed(
    entry: Identified[Ident, InputStreamWithLength],
    decodeResult: Either[DecoderError, T]): Either[ReadError, T] = {
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

  override def put(id: Ident)(t: T): WriteEither =
    for {
      stream <- codec.toStream(t)
      _ <- streamStore.put(id)(stream)
    } yield Identified(id, t)
}
