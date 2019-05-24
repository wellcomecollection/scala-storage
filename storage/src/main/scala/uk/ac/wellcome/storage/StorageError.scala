package uk.ac.wellcome.storage

sealed trait StorageError {
  val e: Throwable
}

sealed trait WriteError extends StorageError
sealed trait EncoderError extends WriteError

case class DaoWriteError(e: Throwable)
  extends WriteError

case class ConditionalWriteError(e: Throwable)
  extends WriteError

case class JsonEncodingError(e: Throwable)
  extends EncoderError

sealed trait ReadError extends StorageError
sealed trait DecoderError extends ReadError

case class DaoReadError(e: Throwable)
  extends ReadError

case class DoesNotExistError(e: Throwable)
  extends ReadError

case class StringDecodingError(e: Throwable)
  extends DecoderError

case class JsonDecodingError(e: Throwable)
  extends DecoderError
