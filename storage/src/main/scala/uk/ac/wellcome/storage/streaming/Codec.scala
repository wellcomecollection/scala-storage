package uk.ac.wellcome.storage.streaming

import java.nio.charset.{Charset, StandardCharsets}

import io.circe
import io.circe.Json
import uk.ac.wellcome.storage.{
  CharsetDecodingError,
  CharsetEncodingError,
  CodecError,
  LossyEncodingDetected
}

import scala.util.{Failure, Success, Try}

trait Codec[T] extends Encoder[T] with Decoder[T]

object Codec {
  import EncoderInstances._
  import DecoderInstances._

  def coerce(charset: Charset)(
    startingString: String): Either[CodecError, String] =
    for {

      // TODO: We don't have any tests for these for comprehensions.
      // If we can find strings that will throw these errors, we should add tests.
      encodedByteBuffer <- Try(charset.encode(startingString)) match {
        case Success(s) => Right(s)
        case Failure(e) => Left(CharsetEncodingError(e))
      }

      decodedCharBuffer <- Try(charset.decode(encodedByteBuffer)) match {
        case Success(s) => Right(s)
        case Failure(e) => Left(CharsetDecodingError(e))
      }

      result <- if (startingString == decodedCharBuffer.array.mkString) {
        Right(decodedCharBuffer.array.mkString)
      } else {
        Left(
          LossyEncodingDetected(
            startingString,
            decodedCharBuffer.array.mkString))
      }
    } yield result

  implicit val bytesCodec: Codec[Array[Byte]] = new Codec[Array[Byte]] {
    override def fromStream(inputStream: FiniteInputStream): DecoderResult[Array[Byte]] =
      bytesDecoder.fromStream(inputStream)

    override def toStream(bytes: Array[Byte]): EncoderResult =
      bytesEncoder.toStream(bytes)
  }

  implicit def stringCodec(
    implicit charset: Charset = StandardCharsets.UTF_8
  ): Codec[String] = new Codec[String] {
    override def fromStream(
      inputStream: FiniteInputStream): DecoderResult[String] =
      stringDecoder.fromStream(inputStream)

    override def toStream(t: String): EncoderResult =
      stringEncoder.toStream(t)
  }

  implicit def jsonCodec: Codec[Json] = new Codec[Json] {
    override def fromStream(
      inputStream: FiniteInputStream): DecoderResult[Json] =
      jsonDecoder.fromStream(inputStream)

    override def toStream(t: Json): EncoderResult =
      jsonEncoder.toStream(t)
  }

  implicit def typeCodec[T](
    implicit
    dec: circe.Decoder[T],
    enc: circe.Encoder[T]
  ): Codec[T] = new Codec[T] {
    override def fromStream(inputStream: FiniteInputStream): DecoderResult[T] =
      typeDecoder.fromStream(inputStream)

    override def toStream(t: T): EncoderResult =
      typeEncoder.toStream(t)
  }

  implicit val streamCodec: Codec[FiniteInputStream] =
    new Codec[FiniteInputStream] {
      override def fromStream(
        inputStream: FiniteInputStream): DecoderResult[FiniteInputStream] =
        streamDecoder.fromStream(inputStream)

      override def toStream(t: FiniteInputStream): EncoderResult =
        streamEncoder.toStream(t)
    }
}
