package uk.ac.wellcome.storage

sealed trait StorageError {
  val e: Throwable
}

/** Used to mark errors that can be retried.
  *
  * For example, a DynamoDB update() might fail with a ConditionalCheckFailed
  * exception if two processes try to write at the same time.  The operation
  * can be retried and will likely succeed.
  *
  */
trait RetryableError

sealed trait CodecError
sealed trait BackendError

sealed trait UpdateError extends StorageError
sealed trait UpdateFunctionError extends UpdateError

case class UpdateNoSourceError(err: NotFoundError) extends UpdateError {
  val e: Throwable = err.e
}
case class UpdateReadError(err: ReadError) extends UpdateError {
  val e: Throwable = err.e
}
case class UpdateWriteError(err: WriteError) extends UpdateError {
  val e: Throwable = err.e
}

case class UpdateNotApplied(e: Throwable) extends UpdateFunctionError
case class UpdateUnexpectedError(e: Throwable) extends UpdateFunctionError

sealed trait WriteError extends StorageError
sealed trait EncoderError extends WriteError

case class StoreWriteError(e: Throwable) extends WriteError with BackendError

case class IncorrectStreamLengthError(e: Throwable = new Error())
    extends DecoderError
    with EncoderError

case class JsonEncodingError(e: Throwable) extends EncoderError

sealed trait ReadError extends StorageError
sealed trait NotFoundError extends ReadError
sealed trait VersionError extends StorageError

case class NoVersionExistsError(e: Throwable = new Error())
    extends VersionError
    with NotFoundError
case class HigherVersionExistsError(e: Throwable = new Error())
    extends VersionError
    with WriteError
case class VersionAlreadyExistsError(e: Throwable = new Error())
    extends VersionError
    with WriteError

sealed trait DecoderError extends ReadError

case class MetadataCoercionFailure(
  failure: List[CodecError],
  success: List[(String, String)],
  e: Throwable = new Error()
) extends WriteError

case class InvalidIdentifierFailure(e: Throwable = new Error())
    extends WriteError

case class DoesNotExistError(e: Throwable = new Error())
    extends NotFoundError
    with BackendError

case class MultipleRecordsError(e: Throwable = new Error())
    extends ReadError
    with BackendError

case class StoreReadError(e: Throwable) extends ReadError with BackendError

case class DanglingHybridStorePointerError(e: Throwable) extends ReadError

case class CannotCloseStreamError(e: Throwable) extends ReadError

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

case class ListingFailure[Location](source: Location,
                                    e: Throwable = new Error())
    extends ReadError
