package uk.ac.wellcome.storage

trait StorageError {
  val e: Throwable
}

sealed trait WriteError extends StorageError
sealed trait EncoderError extends WriteError
case class JsonEncodingError(e: Throwable)
  extends EncoderError


sealed trait ReadError extends StorageError
sealed trait DecoderError extends ReadError

case class StringDecodingError(e: Throwable)
  extends DecoderError

case class JsonDecodingError(e: Throwable)
  extends DecoderError
