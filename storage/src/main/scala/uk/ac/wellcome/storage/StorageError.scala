package uk.ac.wellcome.storage

trait StorageError {
  val e: Throwable
}

sealed trait RetrievalError extends StorageError

sealed trait EncoderError extends RetrievalError

case class JsonEncodingError(e: Throwable) extends EncoderError

sealed trait WriteError extends StorageError

sealed trait DecoderError extends WriteError

case class StringDecodingError(e: Throwable) extends DecoderError

case class JsonDecodingError(e: Throwable) extends DecoderError
