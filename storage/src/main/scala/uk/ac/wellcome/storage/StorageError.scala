package uk.ac.wellcome.storage

sealed trait StorageError {
  val e: Throwable
}

sealed trait CodecError
sealed trait DaoError
sealed trait ObjectStoreError
sealed trait BackendError

sealed trait WriteError extends StorageError
sealed trait EncoderError extends WriteError

case class DaoWriteError(e: Throwable) extends WriteError with DaoError

case class ConditionalWriteError(e: Throwable) extends WriteError with DaoError

case class StoreWriteError(e: Throwable) extends WriteError with BackendError

case class IncorrectStreamLengthError(e: Throwable = new Error())
    extends DecoderError
    with EncoderError

case class JsonEncodingError(e: Throwable) extends EncoderError

case class CharsetEncodingError(e: Throwable = new Error())
    extends CodecError
    with EncoderError

case class LossyEncodingDetected(
  startingString: String,
  decodedString: String,
  e: Throwable = new Error()
) extends CodecError
    with WriteError

sealed trait ReadError extends StorageError
sealed trait DecoderError extends ReadError

case class DaoReadError(e: Throwable) extends ReadError with DaoError

case class MetadataCoercionFailure(
  failure: List[CodecError],
  success: List[(String, String)],
  e: Throwable = new Error()
) extends WriteError

case class DoesNotExistError(e: Throwable = new Error())
    extends ReadError
    with DaoError
    with BackendError

case class StoreReadError(e: Throwable) extends ReadError with BackendError

case class CannotCloseStreamError(e: Throwable)
    extends ReadError
    with ObjectStoreError

case class CharsetDecodingError(e: Throwable = new Error())
    extends CodecError
    with DecoderError

case class ByteDecodingError(e: Throwable) extends DecoderError

case class StringDecodingError(e: Throwable) extends DecoderError

case class JsonDecodingError(e: Throwable) extends DecoderError

sealed trait MaximaError extends ReadError with BackendError

case class MaximaReadError(e: Throwable = new Error())
    extends MaximaError
    with StorageError
case class NoMaximaValueError(e: Throwable = new Error())
    extends MaximaError
    with StorageError

case class ListingFailure[Location](source: Location, e: Throwable = new Error())
    extends ReadError
