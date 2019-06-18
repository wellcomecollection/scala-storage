package uk.ac.wellcome.storage.store.s3

import java.nio.charset.StandardCharsets

import com.amazonaws.SdkClientException
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.{AmazonS3Exception, ObjectMetadata, PutObjectRequest, S3Object}
import com.amazonaws.services.s3.transfer.{TransferManagerBuilder, Upload}
import uk.ac.wellcome.storage._
import uk.ac.wellcome.storage.store.StreamStore
import uk.ac.wellcome.storage.streaming._

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class S3StreamingStore()(implicit s3Client: AmazonS3) extends StreamStore[ObjectLocation, InputStreamWithLengthAndMetadata] {
  type Metadata = Map[String, String]

  override def get(location: ObjectLocation): ReadEither = {
    Try(s3Client.getObject(location.namespace, location.key)) match {
      case Success(retrievedObject: S3Object) => Right(
        Identified(location, buildStoreEntry(retrievedObject))
      )
      case Failure(err) => Left(buildGetError(err))
    }
  }

  private def buildGetError(throwable: Throwable): ReadError =
    throwable match {
      case exc: AmazonS3Exception if exc.getMessage.startsWith("The specified key does not exist") => DoesNotExistError()
      case exc: AmazonS3Exception if exc.getMessage.startsWith("The specified bucket is not valid") => DoesNotExistError()
      case _ => StoreReadError(throwable)
    }

  private def buildStoreEntry(s3Object: S3Object): InputStreamWithLengthAndMetadata = {

    // We get a mutable.Map from the S3 SDK, but we want an immutable Map to pass
    // out to the codebase, hence this slightly odd construction!
    val userMetadata = s3Object.getObjectMetadata.getUserMetadata
    val metadata = userMetadata.keySet().asScala.map { k =>
      (k, userMetadata.get(k))
    }.toMap

    new InputStreamWithLengthAndMetadata(
      s3Object.getObjectContent,
      length = s3Object.getObjectMetadata.getContentLength,
      metadata = metadata
    )
  }

  private val transferManager = TransferManagerBuilder.standard
    .withS3Client(s3Client)
    .build

  // Set bufferReadLimit
  // To prevent 'com.amazonaws.ResetException: Failed to reset the request input stream' being thrown.
  // (see https://github.com/aws/aws-sdk-java/issues/427)
  // (and https://github.com/wellcometrust/platform/issues/3481)
  //
  // If a transfer fails, the AWS SDK retries by rewinding the input stream to the 'mark' set in the buffer at the start.
  // The 'ReadLimit' determines how far the stream can be rewound, if it is smaller than the bytes sent before an error
  // occurs the mark will be invalid and a ResetException is thrown.
  //
  // When using a BufferedInputStream up to 'ReadLimit' bytes are stored in memory, and this must be (at least one byte)
  // larger than the PUT.  The buffer grows dynamically up to this limit.
  //
  // For multipart PUT requests the size must be larger than each PART.
  //
  // To prevent this exception a constant maximum size is set
  // assuming a maximum file of 600GB PUT as 10,000 multipart requests
  // = 60MB ~ 100MB read limit
  // this is a generous estimate and should be sufficient,
  // also given x10 concurrent streams = 10x100MB = 1GB memory overhead which we are comfortable with.
  // This change was tested to reproduce the error with a proxy that dropped traffic to simulate S3 network failure.
  private val MB: Int = 1024 * 1024
  private val bufferReadLimit: Int = 100 * MB

  private def coerceMetadata(metadata: Metadata): Either[MetadataCoercionFailure, Metadata] = {
    val results = metadata.map { case (k, v) =>
      for {
        key <- Codec.coerce(StandardCharsets.US_ASCII)(k)
        value <- Codec.coerce(StandardCharsets.US_ASCII)(v)
      } yield (key, value)
    }

    val failure = results.collect { case Left(e) => e } toList
    val success = results.collect { case Right(kv) => kv } toList

    val mappedSuccess = success.map {
      case (k,v) => k -> v
    }.toMap

    Either.cond(
      failure.isEmpty,
      mappedSuccess,
      MetadataCoercionFailure(failure, success)
    )
  }

  private def createPutObjectRequest(
                                      location: ObjectLocation,
                                      stream: InputStreamWithLengthAndMetadata,
                                      metadataMap: Map[String, String]
  ): PutObjectRequest = {
    val metadata = new ObjectMetadata()

    metadata.setContentLength(stream.length)
    metadata.setUserMetadata(metadataMap.asJava)

    val request = new PutObjectRequest(
      location.namespace,
      location.key,
      stream,
      metadata
    )

    request.getRequestClientOptions.setReadLimit(bufferReadLimit)

    request
  }

  private def uploadWithTransferManager(
    putObjectRequest: PutObjectRequest,
    location: ObjectLocation,
    inputStream: InputStreamWithLengthAndMetadata
  ): Either[WriteError, Identified[ObjectLocation, InputStreamWithLengthAndMetadata]] = Try {
    val upload: Upload = transferManager
      .upload(putObjectRequest)

    upload.waitForUploadResult()
  } match {
    case Success(_) => Right(Identified(location, inputStream))
    case Failure(err) => Left(buildPutError(err))
  }

  override def put(location: ObjectLocation)(inputStream: InputStreamWithLengthAndMetadata): WriteEither = for {
    metadataMap <- coerceMetadata(inputStream.metadata)
    putObjectRequest = createPutObjectRequest(location, inputStream, metadataMap)
    result <- uploadWithTransferManager(putObjectRequest, location, inputStream)
  } yield result

  private def buildPutError(throwable: Throwable): WriteError = throwable match {
    case exc: SdkClientException if exc.getMessage.startsWith("Data read has a different length than the expected") => IncorrectStreamLengthError(exc)
    case exc: SdkClientException if exc.getMessage.startsWith("More data read than expected") => IncorrectStreamLengthError(exc)
    case _ => BackendWriteError(throwable)
  }
}